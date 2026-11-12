package dispatcher

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Environment variables read by NewDispatcher. With NB_SIGNAL_REDIS_URL unset the dispatcher
// behaves like upstream's in-memory one.
const (
	EnvRedisURL          = "NB_SIGNAL_REDIS_URL"
	EnvRedisKeyPrefix    = "NB_SIGNAL_REDIS_KEY_PREFIX"
	EnvRedisPeerTTL      = "NB_SIGNAL_REDIS_PEER_TTL"
	EnvRedisHeartbeat    = "NB_SIGNAL_REDIS_HEARTBEAT"
	EnvRedisTimeout      = "NB_SIGNAL_REDIS_TIMEOUT"
	EnvSignalInstanceID  = "NB_SIGNAL_INSTANCE_ID"
	defaultKeyPrefix     = "nbha:signal:"
	defaultPeerTTL       = 90 * time.Second
	defaultHeartbeat     = 30 * time.Second
	defaultOpTimeout     = time.Second
	defaultProbeInterval = 2 * time.Second
	defaultSelfCheck     = 5 * time.Second
)

// Config configures cross-instance routing. The zero value (no RedisURL and no Client) means
// in-memory only.
type Config struct {
	// RedisURL is redis://, rediss:// or, with a master_name query parameter, a Sentinel URL.
	RedisURL string
	// Client overrides RedisURL; used by tests.
	Client *redis.Client

	KeyPrefix  string
	InstanceID string
	// PeerTTL is the lifetime of a peer->instance key without a heartbeat.
	PeerTTL time.Duration
	// Heartbeat is how often this instance extends the keys of its local peers.
	Heartbeat time.Duration
	// OpTimeout bounds every Redis call made on the request path.
	OpTimeout time.Duration
	// ProbeInterval is how often a degraded instance tries Redis again.
	ProbeInterval time.Duration
	// SelfCheckInterval is how often the instance checks that its own channel still has a
	// subscriber on the current master.
	SelfCheckInterval time.Duration
}

func (c Config) enabled() bool { return c.RedisURL != "" || c.Client != nil }

// ConfigFromEnv reads the NB_SIGNAL_* variables documented in README.md.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		RedisURL:   strings.TrimSpace(os.Getenv(EnvRedisURL)),
		KeyPrefix:  os.Getenv(EnvRedisKeyPrefix),
		InstanceID: strings.TrimSpace(os.Getenv(EnvSignalInstanceID)),
	}
	for _, d := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvRedisPeerTTL, &cfg.PeerTTL},
		{EnvRedisHeartbeat, &cfg.Heartbeat},
		{EnvRedisTimeout, &cfg.OpTimeout},
	} {
		v := strings.TrimSpace(os.Getenv(d.env))
		if v == "" {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("%s: invalid positive duration %q", d.env, v)
		}
		*d.dst = parsed
	}
	return cfg, nil
}

func (c Config) withDefaults() (Config, error) {
	if c.KeyPrefix == "" {
		c.KeyPrefix = defaultKeyPrefix
	}
	if c.PeerTTL <= 0 {
		c.PeerTTL = defaultPeerTTL
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = defaultHeartbeat
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = defaultOpTimeout
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = defaultProbeInterval
	}
	if c.SelfCheckInterval <= 0 {
		c.SelfCheckInterval = defaultSelfCheck
	}
	if c.Heartbeat*2 > c.PeerTTL {
		return c, fmt.Errorf("heartbeat %s must be at most half the peer TTL %s", c.Heartbeat, c.PeerTTL)
	}
	if strings.Contains(c.InstanceID, "|") {
		return c, errors.New("instance ID must not contain '|'")
	}
	if c.InstanceID == "" {
		c.InstanceID = defaultInstanceID()
	}
	return c, nil
}

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "signal"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return strings.ReplaceAll(host, "|", "_") + "-" + hex.EncodeToString(b)
}

// NewRedisClient builds a client from a URL.
//
// Plain URLs are parsed by go-redis ParseURL (redis://[user:pass@]host:port[/db], rediss:// for TLS).
// A URL with a master_name query parameter selects Sentinel through NewFailoverClient, in the
// format of go-redis ParseFailoverURL (v9.10+):
//
//	redis[s]://[user:pass@]sentinel1:26379[/db]?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379
//
// The host and every addr are Sentinel addresses; the user info authenticates to the data nodes.
// Extra parameters: sentinel_username, sentinel_password, db, dial_timeout, read_timeout,
// write_timeout, pool_size, skip_verify (rediss only). Unknown parameters are an error.
func NewRedisClient(rawURL string) (*redis.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	if !u.Query().Has("master_name") {
		opt, err := redis.ParseURL(rawURL)
		if err != nil {
			return nil, err
		}
		return redis.NewClient(opt), nil
	}
	opt, err := parseFailoverURL(u)
	if err != nil {
		return nil, err
	}
	return redis.NewFailoverClient(opt), nil
}

func parseFailoverURL(u *url.URL) (*redis.FailoverOptions, error) {
	o := &redis.FailoverOptions{}
	switch u.Scheme {
	case "redis":
	case "rediss":
		o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	default:
		return nil, fmt.Errorf("redis URL: invalid scheme %q", u.Scheme)
	}
	if u.User != nil {
		o.Username = u.User.Username()
		o.Password, _ = u.User.Password()
	}
	addrs := []string{}
	if u.Host != "" {
		port := u.Port()
		if port == "" {
			port = "26379"
		}
		addrs = append(addrs, net.JoinHostPort(u.Hostname(), port))
	}
	if p := strings.Trim(u.Path, "/"); p != "" {
		db, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("redis URL: invalid database %q", p)
		}
		o.DB = db
	}
	for k, vs := range u.Query() {
		v := vs[len(vs)-1]
		var err error
		switch k {
		case "master_name":
			o.MasterName = v
		case "addr":
			for _, a := range vs {
				h, p, splitErr := net.SplitHostPort(a)
				if splitErr != nil || h == "" || p == "" {
					return nil, fmt.Errorf("redis URL: invalid addr %q", a)
				}
				addrs = append(addrs, net.JoinHostPort(h, p))
			}
		case "sentinel_username":
			o.SentinelUsername = v
		case "sentinel_password":
			o.SentinelPassword = v
		case "db":
			o.DB, err = strconv.Atoi(v)
		case "dial_timeout":
			o.DialTimeout, err = parseURLDuration(v)
		case "read_timeout":
			o.ReadTimeout, err = parseURLDuration(v)
		case "write_timeout":
			o.WriteTimeout, err = parseURLDuration(v)
		case "pool_size":
			o.PoolSize, err = strconv.Atoi(v)
		case "skip_verify":
			if o.TLSConfig != nil {
				o.TLSConfig.InsecureSkipVerify, err = strconv.ParseBool(v)
			}
		default:
			return nil, fmt.Errorf("redis URL: unexpected option %q", k)
		}
		if err != nil {
			return nil, fmt.Errorf("redis URL: invalid %s %q: %w", k, v, err)
		}
	}
	if o.MasterName == "" {
		return nil, errors.New("redis URL: master_name is empty")
	}
	if len(addrs) == 0 {
		return nil, errors.New("redis URL: no Sentinel address")
	}
	o.SentinelAddrs = addrs
	return o, nil
}

// parseURLDuration accepts Go durations and, like go-redis, plain integers as seconds.
func parseURLDuration(v string) (time.Duration, error) {
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return time.ParseDuration(v)
}
