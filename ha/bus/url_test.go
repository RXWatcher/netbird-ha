package bus

import (
	"testing"

	"github.com/netbirdio/signal-dispatcher/dispatcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Bloem-Studios/netbird-ha/ha/cache"
)

// TestURLFormatMatchesCacheAndDispatcher keeps NB_HA_REDIS_URL in the format of
// NB_CACHE_REDIS_ADDRESS (ha/cache) and NB_SIGNAL_REDIS_URL (dispatcher/): every URL
// is accepted or refused by all three alike, and accepted ones reach the same data
// nodes with the same credentials, database and TLS setting. The cases mirror
// ha/cache's TestURLFormatMatchesDispatcher; add new ones to both.
func TestURLFormatMatchesCacheAndDispatcher(t *testing.T) {
	for _, raw := range []string{
		"redis://localhost:6379",
		"redis://user:pw@10.0.0.1:6380/2",
		"rediss://:pw@cache:6380/1",
		"redis://localhost:6379/0?dial_timeout=3s&pool_size=5",
		"unix:///run/redis.sock",
		"redis://s1:26379?master_name=m",
		"redis://sentinel?master_name=m",
		"rediss://user:pw@s1:26380/3?master_name=mymaster&addr=s2:26379&addr=[fd00::1]:26379&sentinel_password=spw&sentinel_username=su&dial_timeout=3&read_timeout=500ms&skip_verify=true&pool_size=7",
		"redis://u:p@s1:26379/4?master_name=m&db=5",
		"redis://s1?master_name=",
		"redis://s1?master_name=m&bogus=1",
		"redis://s1?master_name=m&route_by_latency=true",
		"redis://s1?master_name=m&addr=nohost",
		"redis://s1?master_name=m&db=x",
		"http://s1?master_name=m",
		"http://localhost:6379",
		"redis:///?master_name=m",
		"redis://localhost:6379/notadb",
		// The README's two-instance example.
		"redis://:valkeypw@sentinel1:26379?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379&sentinel_password=sentinelpw",
		"redis://:valkeypw@sentinel1:26379?master_name=mymaster&addr=sentinel2:26379&sentinel_password=sentinelpw&db=2",
		"redis://localhost:6379?db=3",
	} {
		t.Run(raw, func(t *testing.T) {
			sig, sigErr := dispatcher.NewRedisClient(raw)
			cch, _, cacheErr := cache.NewClient(raw, 10)
			ours, ourErr := NewClient(raw)
			require.Equal(t, sigErr == nil, ourErr == nil, "dispatcher err=%v, bus err=%v", sigErr, ourErr)
			require.Equal(t, cacheErr == nil, ourErr == nil, "cache err=%v, bus err=%v", cacheErr, ourErr)
			if ourErr != nil {
				return
			}
			defer sig.Close()
			defer cch.Close()
			defer ours.Close()
			b := ours.Options()
			for name, a := range map[string]*struct {
				Addr, Username, Password string
				DB                       int
				TLS                      bool
			}{
				"dispatcher": {sig.Options().Addr, sig.Options().Username, sig.Options().Password, sig.Options().DB, sig.Options().TLSConfig != nil},
				"cache":      {cch.Options().Addr, cch.Options().Username, cch.Options().Password, cch.Options().DB, cch.Options().TLSConfig != nil},
			} {
				assert.Equal(t, a.Addr, b.Addr, name)
				assert.Equal(t, a.Username, b.Username, name)
				assert.Equal(t, a.Password, b.Password, name)
				assert.Equal(t, a.DB, b.DB, name)
				assert.Equal(t, a.TLS, b.TLSConfig != nil, name)
			}
		})
	}
}
