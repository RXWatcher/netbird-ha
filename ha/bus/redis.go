package bus

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// Environment variables read by ConfigFromEnv (names, not values).
//
//nolint:gosec // G101: these are variable names
const (
	EnvRedisURL         = "NB_HA_REDIS_URL"             // redis://[user:pass@]host:port/db or rediss://
	EnvSentinelAddrs    = "NB_HA_REDIS_SENTINEL_ADDRS"  // host:port,host:port
	EnvSentinelMaster   = "NB_HA_REDIS_SENTINEL_MASTER" // master name, required with addrs
	EnvSentinelPassword = "NB_HA_REDIS_SENTINEL_PASSWORD"
	EnvRedisUsername    = "NB_HA_REDIS_USERNAME" // Sentinel mode only; the URL carries its own
	EnvRedisPassword    = "NB_HA_REDIS_PASSWORD" // Sentinel mode only
	EnvRedisDB          = "NB_HA_REDIS_DB"       // Sentinel mode only
	EnvRedisTLS         = "NB_HA_REDIS_TLS"      // Sentinel mode only: "true" for TLS to master and sentinels
	EnvChannel          = "NB_HA_BUS_CHANNEL"
	EnvInstanceID       = "NB_HA_INSTANCE_ID"

	DefaultChannel = "netbird-ha:mgmt:updates"
)

// Config selects a plain Redis/Valkey server (URL) or a Sentinel-managed master.
type Config struct {
	URL string

	SentinelAddrs    []string
	SentinelMaster   string
	SentinelPassword string
	Username         string
	Password         string
	DB               int
	TLS              bool

	Channel string
	Origin  string

	PublishTimeout time.Duration // default 2s
	PingInterval   time.Duration // default 15s; two unanswered pings force a reconnect
	MaxBackoff     time.Duration // default 5s
}

// ConfigFromEnv returns the bus config and whether HA messaging is enabled at all
// (NB_HA_REDIS_URL or NB_HA_REDIS_SENTINEL_ADDRS set).
func ConfigFromEnv() (Config, bool, error) {
	c := Config{
		URL:              os.Getenv(EnvRedisURL),
		SentinelMaster:   os.Getenv(EnvSentinelMaster),
		SentinelPassword: os.Getenv(EnvSentinelPassword),
		Username:         os.Getenv(EnvRedisUsername),
		Password:         os.Getenv(EnvRedisPassword),
		Channel:          os.Getenv(EnvChannel),
		Origin:           os.Getenv(EnvInstanceID),
		TLS:              strings.EqualFold(os.Getenv(EnvRedisTLS), "true"),
	}
	for _, a := range strings.Split(os.Getenv(EnvSentinelAddrs), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.SentinelAddrs = append(c.SentinelAddrs, a)
		}
	}
	if v := os.Getenv(EnvRedisDB); v != "" {
		db, err := strconv.Atoi(v)
		if err != nil {
			return c, false, fmt.Errorf("%s: %w", EnvRedisDB, err)
		}
		c.DB = db
	}
	if c.URL == "" && len(c.SentinelAddrs) == 0 {
		return c, false, nil
	}
	return c, true, c.validate()
}

func (c *Config) validate() error {
	if c.URL != "" && len(c.SentinelAddrs) > 0 {
		return fmt.Errorf("set either %s or %s, not both", EnvRedisURL, EnvSentinelAddrs)
	}
	if len(c.SentinelAddrs) > 0 && c.SentinelMaster == "" {
		return fmt.Errorf("%s needs %s", EnvSentinelAddrs, EnvSentinelMaster)
	}
	return nil
}

func (c *Config) defaults() {
	if c.Channel == "" {
		c.Channel = DefaultChannel
	}
	if c.Origin == "" {
		c.Origin = defaultOrigin()
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 2 * time.Second
	}
	if c.PingInterval <= 0 {
		c.PingInterval = 15 * time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Second
	}
}

// defaultOrigin is hostname plus a random suffix, so a restarted instance never
// mistakes the previous process's messages for its own.
func defaultOrigin() string {
	host, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

// NewClient builds a go-redis client: a Sentinel failover client when sentinel
// addresses are set, otherwise a plain client from the URL.
func NewClient(c Config) (redis.UniversalClient, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if len(c.SentinelAddrs) > 0 {
		opts := &redis.FailoverOptions{
			MasterName:       c.SentinelMaster,
			SentinelAddrs:    c.SentinelAddrs,
			SentinelPassword: c.SentinelPassword,
			Username:         c.Username,
			Password:         c.Password,
			DB:               c.DB,
		}
		if c.TLS {
			opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return redis.NewFailoverClient(opts), nil
	}
	if c.URL == "" {
		return nil, errors.New("no Redis URL or Sentinel addresses configured")
	}
	opts, err := redis.ParseURL(c.URL)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", EnvRedisURL, err)
	}
	return redis.NewClient(opts), nil
}

// Redis is a Bus on Redis/Valkey pub/sub. Pub/sub is fire-and-forget: a message
// published while an instance is disconnected is lost to it, which Run reports
// through onSubscribed.
type Redis struct {
	cfg     Config
	client  redis.UniversalClient
	metrics Metrics
}

// NewRedis connects lazily; nothing is dialled until Publish or Run.
func NewRedis(cfg Config) (*Redis, error) {
	cfg.defaults()
	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Redis{cfg: cfg, client: client}, nil
}

func (r *Redis) Origin() string                { return r.cfg.Origin }
func (r *Redis) Metrics() *Metrics             { return &r.metrics }
func (r *Redis) Close() error                  { return r.client.Close() }
func (r *Redis) Channel() string               { return r.cfg.Channel }
func (r *Redis) Client() redis.UniversalClient { return r.client }

func (r *Redis) Publish(ctx context.Context, e Envelope) error {
	e.V = Version
	e.Origin = r.cfg.Origin
	e.SentAt = time.Now().UTC()
	b, err := encode(e)
	if err != nil {
		r.metrics.PublishErrors.Add(1)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	if err := r.client.Publish(ctx, r.cfg.Channel, b).Err(); err != nil {
		r.metrics.PublishErrors.Add(1)
		return fmt.Errorf("publish %s for account %s: %w", e.Kind, e.Account, err)
	}
	r.metrics.Published.Add(1)
	return nil
}

func (r *Redis) Run(ctx context.Context, h Handler, onSubscribed func(ctx context.Context)) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		healthy := r.session(ctx, h, onSubscribed)
		r.metrics.Connected.Store(0)
		if ctx.Err() != nil {
			return
		}
		if healthy {
			backoff = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, r.cfg.MaxBackoff)
	}
}

// session runs one subscription on a fresh PubSub until it fails. It returns true
// when the subscription had been established, so Run resets its backoff.
func (r *Redis) session(ctx context.Context, h Handler, onSubscribed func(ctx context.Context)) bool {
	ps := r.client.Subscribe(ctx, r.cfg.Channel)
	defer func() { _ = ps.Close() }()

	subscribed := false
	pingOutstanding := false
	for {
		msg, err := ps.ReceiveTimeout(ctx, r.cfg.PingInterval)
		if err != nil {
			if ctx.Err() != nil {
				return subscribed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() && subscribed {
				if pingOutstanding {
					log.Warnf("ha bus: no reply from Redis within %s, reconnecting", 2*r.cfg.PingInterval)
					r.metrics.ReceiveErrors.Add(1)
					return true
				}
				if err := ps.Ping(ctx); err != nil {
					log.Warnf("ha bus: ping failed, reconnecting: %v", err)
					r.metrics.ReceiveErrors.Add(1)
					return true
				}
				pingOutstanding = true
				continue
			}
			r.metrics.ReceiveErrors.Add(1)
			log.Warnf("ha bus: receive on %s failed, reconnecting: %v", r.cfg.Channel, err)
			return subscribed
		}
		pingOutstanding = false

		switch m := msg.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" && m.Channel == r.cfg.Channel {
				subscribed = true
				r.metrics.Subscriptions.Add(1)
				r.metrics.Connected.Store(1)
				log.Infof("ha bus: subscribed to %s as %s", r.cfg.Channel, r.cfg.Origin)
				if onSubscribed != nil {
					onSubscribed(ctx)
				}
			}
		case *redis.Message:
			r.deliver(ctx, []byte(m.Payload), h)
		case *redis.Pong:
		}
	}
}

func (r *Redis) deliver(ctx context.Context, payload []byte, h Handler) {
	e, err := decode(payload)
	if err != nil {
		r.metrics.DroppedInvalid.Add(1)
		log.Warnf("ha bus: dropping message: %v", err)
		return
	}
	if e.Origin == r.cfg.Origin {
		return
	}
	r.metrics.Received.Add(1)
	h(ctx, e)
}
