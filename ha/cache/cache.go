// Package cache is upstream NetBird's shared cache store (nbcache.Store) on
// Redis or Valkey, with Sentinel support.
//
// Upstream builds its Redis cache from NB_CACHE_REDIS_ADDRESS with
// redis.ParseURL, which names exactly one server, so a Sentinel failover
// leaves every instance writing to a replica or to nothing. This package reads
// the same variable and accepts exactly the URL format of the signal
// dispatcher's NB_SIGNAL_REDIS_URL (dispatcher/config.go, NewRedisClient), so
// operators write one Redis URL for both:
//
//   - redis://[user:password@]host:6379[/db], or rediss:// for TLS: one
//     server, parsed by go-redis ParseURL as upstream does.
//   - With a master_name query parameter, a Sentinel deployment:
//     redis[s]://[user:password@]sentinel1:26379[/db]?master_name=mymaster&addr=sentinel2:26379&addr=sentinel3:26379
//     The host and each addr are Sentinels (port defaults to 26379); the user
//     info authenticates to the data nodes. Options: sentinel_username,
//     sentinel_password, db, dial_timeout, read_timeout, write_timeout (Go
//     durations or plain seconds), pool_size, and skip_verify for rediss. Any
//     other option is an error. This is go-redis's ParseFailoverURL format.
//
// The store keeps upstream's semantics: SetNX is Redis SET NX with a TTL, and
// GetDel is Redis GETDEL, so a one-time value is consumed by exactly one
// caller across all instances.
package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"

	nbcache "github.com/netbirdio/netbird/management/server/cache"
)

// SentinelMasterParam is the query parameter that makes a URL name a
// Sentinel deployment.
const SentinelMasterParam = "master_name"

const pingTimeout = 5 * time.Second

// Store is nbcache.Store on a go-redis client, which is a Sentinel failover
// client when the URL names a Sentinel deployment.
type Store struct {
	store.StoreInterface
	client   *redis.Client
	sentinel bool
}

var _ nbcache.Store = (*Store)(nil)

// New builds a store from rawURL with upstream's pool sizing for maxConn
// connections and checks that the server answers. The error never quotes the
// URL, which can carry passwords.
func New(ctx context.Context, rawURL string, maxConn int) (*Store, error) {
	client, sentinel, err := NewClient(rawURL, maxConn)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis cache: ping: %w", err)
	}
	if sentinel {
		log.WithContext(ctx).Info("using redis cache through Sentinel")
	} else {
		log.WithContext(ctx).Infof("using redis cache at %s", client.Options().Addr)
	}
	return &Store{StoreInterface: redisstore.NewRedis(client), client: client, sentinel: sentinel}, nil
}

// NewClient parses rawURL and builds its client without connecting. It
// reports whether the URL names a Sentinel deployment.
func NewClient(rawURL string, maxConn int) (*redis.Client, bool, error) {
	options, failover, err := ParseURL(rawURL)
	if err != nil {
		return nil, false, err
	}
	if failover != nil {
		failover.MaxIdleConns, failover.MinIdleConns, failover.MaxActiveConns = poolSizes(maxConn)
		failover.ConnMaxIdleTime = 30 * time.Minute
		failover.ConnMaxLifetime = 0
		failover.PoolTimeout = 10 * time.Second
		return redis.NewFailoverClient(failover), true, nil
	}
	options.MaxIdleConns, options.MinIdleConns, options.MaxActiveConns = poolSizes(maxConn)
	options.ConnMaxIdleTime = 30 * time.Minute
	options.ConnMaxLifetime = 0
	options.PoolTimeout = 10 * time.Second
	return redis.NewClient(options), false, nil
}

// poolSizes is upstream's sizing (management/server/cache/redis.go).
func poolSizes(maxConn int) (maxIdle, minIdle, maxActive int) {
	return int(math.Ceil(float64(maxConn) * 0.5)), int(math.Ceil(float64(maxConn) * 0.1)), maxConn
}

// ParseURL returns exactly one of single-server options and failover
// options. It mirrors dispatcher.NewRedisClient (dispatcher/config.go); a
// parity test keeps the two in step.
func ParseURL(raw string) (*redis.Options, *redis.FailoverOptions, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse quotes the URL, which can carry a password.
		return nil, nil, errors.New("redis cache: the URL cannot be parsed")
	}
	if !u.Query().Has(SentinelMasterParam) {
		options, err := redis.ParseURL(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("redis cache: %w", err)
		}
		return options, nil, nil
	}
	failover, err := parseFailoverURL(u)
	if err != nil {
		return nil, nil, fmt.Errorf("redis cache: %w", err)
	}
	if failover.DialTimeout == 0 {
		// The Sentinel dialer does not fill in go-redis's 5 s default, and a
		// dial to a master that is gone would otherwise wait for the kernel.
		failover.DialTimeout = 5 * time.Second
	}
	return nil, failover, nil
}

// parseFailoverURL is dispatcher/config.go's parseFailoverURL.
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
		case SentinelMasterParam:
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
			// The value may be a password; name only the option.
			return nil, fmt.Errorf("redis URL: invalid %s: %w", k, err)
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

// parseURLDuration accepts Go durations and, like go-redis, plain integers as
// seconds.
func parseURLDuration(v string) (time.Duration, error) {
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	return time.ParseDuration(v)
}

// SetNX stores value under key with ttl only when key does not exist.
func (s *Store) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, key, value, ttl).Result()
}

// GetDel reads key and deletes it in one command, so only one caller can
// consume it.
func (s *Store) GetDel(ctx context.Context, key string) (string, bool, error) {
	value, err := s.client.GetDel(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// Client returns the underlying client.
func (s *Store) Client() *redis.Client {
	return s.client
}

// Close closes the client.
func (s *Store) Close() error {
	return s.client.Close()
}
