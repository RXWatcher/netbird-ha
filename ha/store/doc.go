// Package store makes upstream NetBird's SQL store safe to share between
// several management instances on one PostgreSQL database.
//
// Two things in upstream assume a single process:
//
//   - AcquireGlobalLock is a sync.Mutex. It guards account creation
//     (GetOrCreateAccountByUser, GetOrCreateAccountByPrivateDomain and the
//     private-domain path of getAccountIDWithAuthorizationClaims), so two
//     instances handling the same first login at once each create an account.
//     Store replaces it with a PostgreSQL session advisory lock
//     (GlobalLockKey) held on a dedicated connection.
//
//   - Every start runs upstream's migrations (store.NewSqlStore with
//     skipMigration=false). OpenSQLStore runs them under another advisory
//     lock (MigrationLockKey): the first instance of a release migrates and
//     records the release in netbird_ha_schema_migrations; the others wait
//     for the lock, find the record and open with skipMigration=true.
//
// # Rolling upgrades
//
// Upstream's migrations are not all additive. Some drop columns
// (migration.go: DropColumn in the agent-network and cost migrations) and
// some rewrite data in place (gob to JSON, setup-key hashing, public-ID
// backfills). The instance that starts first on a new release migrates the
// shared database while instances of the old release are still serving.
//
// Before each upgrade, review the migration diff between the two upstream
// tags (management/server/store/store.go getMigrationsPreAuto and
// getMigrationsPostAuto, management/server/migration, and the AutoMigrate
// model list in sql_store.go):
//
//   - Additive only (new tables, new nullable or defaulted columns, new
//     indexes): upgrade one instance at a time. Drain it, stop it, start the
//     new release (it migrates under the lock), check health, move on. Old
//     instances keep working because gorm ignores columns it does not know.
//
//   - Destructive (DropColumn, DropTable, a renamed column, a changed column
//     type, or a data rewrite the old release cannot read): old instances
//     fail on queries that name the dropped or rewritten column. Stop all
//     instances, start one on the new release, then start the rest. Tunnels
//     keep working while the control plane is down; clients reconnect with
//     backoff.
//
// Never roll back to an older release after a destructive migration: its
// migrations are recorded as done and it would run on a schema it does not
// know. Restore the database instead.
//
// Set NB_HA_FORCE_MIGRATIONS=true on one instance to run the migrations
// again (still under the lock), for example after restoring a dump taken
// before the release was recorded.
//
// Upstream's other databases (the activity event store and the reverse-proxy
// access-log store) still migrate on every start, outside these locks. Their
// migrations are additive, but two instances creating the same tables at the
// same moment on a fresh database can fail; start the first instance alone.
//
// # Connection requirements
//
// Session advisory locks need a session: connect directly to PostgreSQL (a
// multi-host DSN with target_session_attrs=read-write works with
// pg_auto_failover) or through a session-mode pooler. A transaction-mode
// pooler breaks them. A failover ends every session and so releases every
// lock; an instance inside the global lock at that moment logs ErrLockLost.
package store
