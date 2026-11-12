package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/netbirdio/netbird/management/internals/controllers/network_map/controller"
	"github.com/netbirdio/netbird/management/internals/controllers/network_map/update_channel"
	"github.com/netbirdio/netbird/management/internals/modules/peers"
	ephemeralmanager "github.com/netbirdio/netbird/management/internals/modules/peers/ephemeral/manager"
	"github.com/netbirdio/netbird/management/internals/server/config"
	"github.com/netbirdio/netbird/management/internals/shared/db"
	nbserver "github.com/netbirdio/netbird/management/server"
	"github.com/netbirdio/netbird/management/server/activity"
	nbcache "github.com/netbirdio/netbird/management/server/cache"
	"github.com/netbirdio/netbird/management/server/job"
	"github.com/netbirdio/netbird/management/server/permissions"
	"github.com/netbirdio/netbird/management/server/settings"
	nbstore "github.com/netbirdio/netbird/management/server/store"
	"github.com/netbirdio/netbird/management/server/telemetry"
	"github.com/netbirdio/netbird/management/server/types"
	"github.com/netbirdio/netbird/shared/auth"

	"github.com/Bloem-Studios/netbird-ha/ha/store/storetest"
)

func openConn(t *testing.T, dsn string) *db.Conn {
	t.Helper()
	conn, err := db.OpenPostgres(context.Background(), dsn, db.PoolConfig{MaxConns: 10, MinConns: 1, MaxConnLifetime: time.Hour, HealthCheckPeriod: time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}

// TestMigrationsRunOnceAcrossInstances starts several instances of one
// release at once on an empty database: exactly one runs upstream's
// migrations, the others wait and open the migrated schema.
func TestMigrationsRunOnceAcrossInstances(t *testing.T) {
	dsn := storetest.NewDatabase(t)
	const instances = 4
	const version = "v0.82.1"

	var wg sync.WaitGroup
	migrated := make([]bool, instances)
	stores := make([]*nbstore.SqlStore, instances)
	errs := make([]error, instances)
	start := make(chan struct{})
	for i := range instances {
		conn := openConn(t, dsn)
		locker := newLocker(t, dsn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			stores[i], migrated[i], errs[i] = OpenSQLStore(context.Background(), conn, nil, locker,
				MigrationOptions{Version: version, Instance: fmt.Sprintf("mgmt-%d", i)})
		}()
	}
	close(start)
	wg.Wait()

	ran := 0
	for i := range instances {
		require.NoError(t, errs[i], "instance %d", i)
		if migrated[i] {
			ran++
		}
		// Every instance sees the full schema.
		_, err := stores[i].GetAccountsCounter(context.Background())
		require.NoError(t, err, "instance %d", i)
	}
	assert.Equal(t, 1, ran, "instances that ran the migrations")

	locker := newLocker(t, dsn)
	assert.Equal(t, 1, countRows(t, locker.Pool(), `SELECT count(*) FROM netbird_ha_schema_migrations WHERE version = $1`, version))

	// A restart of the same release skips them.
	_, again, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, locker, MigrationOptions{Version: version})
	require.NoError(t, err)
	assert.False(t, again)

	// A new release runs them once more.
	_, next, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, locker, MigrationOptions{Version: "v0.82.2"})
	require.NoError(t, err)
	assert.True(t, next)

	// Force reruns them.
	_, forced, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, locker, MigrationOptions{Version: version, Force: true})
	require.NoError(t, err)
	assert.True(t, forced)

	// A development build always migrates, still under the lock.
	_, devel, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, locker, MigrationOptions{Version: "(devel)"})
	require.NoError(t, err)
	assert.True(t, devel)
	assert.Equal(t, 0, countRows(t, locker.Pool(), `SELECT count(*) FROM netbird_ha_schema_migrations WHERE version = '(devel)'`))
}

func TestUpstreamVersionIsKnown(t *testing.T) {
	// The test binary is built with the module's dependencies, so this is
	// the version a release build reports.
	v := UpstreamVersion()
	require.NotEmpty(t, v)
	t.Logf("upstream version: %s", v)
	assert.True(t, isReleaseVersion(v), v)
}

// instance is one management server's account manager on the shared
// database.
func newAccountManager(t *testing.T, st nbstore.Store, singleAccountDomain string) *nbserver.DefaultAccountManager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	metrics, err := telemetry.NewDefaultAppMetrics(ctx)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	settingsManager := settings.NewMockManager(ctrl)
	settingsManager.EXPECT().GetExtraSettings(gomock.Any(), gomock.Any()).Return(&types.ExtraSettings{}, nil).AnyTimes()
	settingsManager.EXPECT().UpdateExtraSettings(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	permissionsManager := permissions.NewManager(st)
	peersManager := peers.NewManager(st, permissionsManager)
	cacheStore, err := nbcache.NewStore(ctx, nbcache.DefaultStoreMaxTimeout, 0, 100)
	require.NoError(t, err)

	updateManager := update_channel.NewPeersUpdateManager(metrics)
	requestBuffer := nbserver.NewAccountRequestBuffer(ctx, st)
	nmc := controller.NewController(ctx, st, metrics, updateManager, requestBuffer, nbserver.MockIntegratedValidator{}, settingsManager,
		"netbird.cloud", ephemeralmanager.NewEphemeralManager(st, peersManager), &config.Config{}, nil)
	am, err := nbserver.BuildManager(ctx, &config.Config{}, st, nmc, job.NewJobManager(nil, st, peersManager), nil, singleAccountDomain,
		&activity.InMemoryEventStore{}, nil, false, nbserver.MockIntegratedValidator{}, metrics, settingsManager, permissionsManager, false, cacheStore)
	require.NoError(t, err)
	return am
}

// loginRace runs first logins for users on two instances at once, each
// login on both instances concurrently, and returns how many accounts the
// database ends up with.
func loginRace(t *testing.T, wrap bool, users []string) int {
	t.Helper()
	dsn := storetest.NewDatabase(t)
	locker := newLocker(t, dsn)
	_, _, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, locker, MigrationOptions{Version: "v0.82.1"})
	require.NoError(t, err)

	const domain = "netbird.selfhosted"
	managers := make([]*nbserver.DefaultAccountManager, 2)
	for i := range managers {
		sqlStore, _, err := OpenSQLStore(context.Background(), openConn(t, dsn), nil, newLocker(t, dsn), MigrationOptions{Version: "v0.82.1"})
		require.NoError(t, err)
		var st nbstore.Store = sqlStore
		if wrap {
			st = Wrap(sqlStore, newLocker(t, dsn), nil)
		}
		// Self-hosted single-account mode: every user belongs to one
		// private-domain account, created by the first login.
		managers[i] = newAccountManager(t, st, domain)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, user := range users {
		for _, am := range managers {
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, _, err := am.GetAccountIDFromUserAuth(context.Background(), auth.UserAuth{UserId: user, Domain: domain})
					assert.NoError(t, err)
				}()
			}
		}
	}
	close(start)
	wg.Wait()

	return countRows(t, locker.Pool(), `SELECT count(*) FROM accounts`)
}

func users(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("user-%d", i)
	}
	return out
}

// TestFirstLoginRaceCreatesOneAccount is the design's scenario 6: the same
// new user (and other new users) logging in on two instances at once
// creates exactly one account.
func TestFirstLoginRaceCreatesOneAccount(t *testing.T) {
	for round := range 5 {
		accounts := loginRace(t, true, users(8))
		require.Equal(t, 1, accounts, "round %d: accounts after concurrent first logins", round)
	}
}

// TestFirstLoginRaceWithoutHAStore shows the bug the HA store fixes. It only
// logs, because the race does not lose every time.
func TestFirstLoginRaceWithoutHAStore(t *testing.T) {
	worst := 0
	for range 5 {
		worst = max(worst, loginRace(t, false, users(8)))
	}
	t.Logf("upstream store, two instances: up to %d accounts for one single-account domain", worst)
}
