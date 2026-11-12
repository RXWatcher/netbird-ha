package dispatcher

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EnvRedisURL, "redis://localhost:6379/0")
	t.Setenv(EnvRedisPeerTTL, "60s")
	t.Setenv(EnvRedisHeartbeat, "20s")
	t.Setenv(EnvRedisTimeout, "")
	t.Setenv(EnvSignalInstanceID, "sig-1")
	t.Setenv(EnvRedisKeyPrefix, "x:")
	cfg, err := ConfigFromEnv()
	require.NoError(t, err)
	cfg, err = cfg.withDefaults()
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, cfg.PeerTTL)
	require.Equal(t, 20*time.Second, cfg.Heartbeat)
	require.Equal(t, defaultOpTimeout, cfg.OpTimeout)
	require.Equal(t, "sig-1", cfg.InstanceID)
	require.Equal(t, "x:", cfg.KeyPrefix)

	t.Setenv(EnvRedisPeerTTL, "soon")
	_, err = ConfigFromEnv()
	require.Error(t, err)
}

func TestConfigValidation(t *testing.T) {
	_, err := Config{PeerTTL: 30 * time.Second, Heartbeat: 20 * time.Second}.withDefaults()
	require.Error(t, err, "heartbeat must leave room for a missed beat")
	_, err = Config{InstanceID: "a|b"}.withDefaults()
	require.Error(t, err)
	cfg, err := Config{}.withDefaults()
	require.NoError(t, err)
	require.NotEmpty(t, cfg.InstanceID)
	other, _ := Config{}.withDefaults()
	require.NotEqual(t, cfg.InstanceID, other.InstanceID, "default instance IDs must differ between processes")
}

func TestNewRedisClientPlain(t *testing.T) {
	c, err := NewRedisClient("redis://user:pw@10.0.0.1:6380/2")
	require.NoError(t, err)
	defer c.Close()
	require.Equal(t, "10.0.0.1:6380", c.Options().Addr)
	require.Equal(t, 2, c.Options().DB)
	require.Equal(t, "pw", c.Options().Password)
}

func TestParseFailoverURL(t *testing.T) {
	u := "rediss://user:pw@s1:26380/3?master_name=mymaster&addr=s2:26379&addr=[fd00::1]:26379" +
		"&sentinel_password=spw&sentinel_username=su&dial_timeout=3&read_timeout=500ms&skip_verify=true&pool_size=7"
	o, err := parseFailoverURL(mustURL(t, u))
	require.NoError(t, err)
	require.Equal(t, "mymaster", o.MasterName)
	require.Equal(t, []string{"s1:26380", "s2:26379", "[fd00::1]:26379"}, o.SentinelAddrs)
	require.Equal(t, "user", o.Username)
	require.Equal(t, "pw", o.Password)
	require.Equal(t, "su", o.SentinelUsername)
	require.Equal(t, "spw", o.SentinelPassword)
	require.Equal(t, 3, o.DB)
	require.Equal(t, 3*time.Second, o.DialTimeout)
	require.Equal(t, 500*time.Millisecond, o.ReadTimeout)
	require.Equal(t, 7, o.PoolSize)
	require.NotNil(t, o.TLSConfig)
	require.True(t, o.TLSConfig.InsecureSkipVerify)

	o, err = parseFailoverURL(mustURL(t, "redis://sentinel?master_name=m"))
	require.NoError(t, err)
	require.Equal(t, []string{"sentinel:26379"}, o.SentinelAddrs)
	require.Nil(t, o.TLSConfig)

	for _, bad := range []string{
		"redis://s1?master_name=",
		"redis://s1?master_name=m&bogus=1",
		"redis://s1?master_name=m&addr=nohost",
		"http://s1?master_name=m",
		"redis:///?master_name=m",
	} {
		_, err := NewRedisClient(bad)
		require.Error(t, err, bad)
	}
	c, err := NewRedisClient("redis://s1:26379?master_name=m")
	require.NoError(t, err)
	_ = c.Close()
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	require.NoError(t, err)
	return u
}
