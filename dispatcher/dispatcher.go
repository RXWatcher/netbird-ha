// Derived from github.com/netbirdio/signal-dispatcher/dispatcher at 6b4ac470ca45
// (Copyright (c) 2024, netbirdio; BSD 3-Clause, see LICENSE and AUTHORS in this directory).
// The exported API is unchanged so NetBird's signal server builds against this module
// through a replace directive. Cross-instance routing over Redis is in redis.go.

// Package dispatcher routes signal messages to the stream of their destination peer, on this
// instance or, when NB_SIGNAL_REDIS_URL is set, on another signal instance through Redis.
package dispatcher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

// remoteQueueSize bounds the messages from other instances waiting for one local peer.
const remoteQueueSize = 128

type Dispatcher struct {
	peerChannels map[string]*peerChan
	mu           sync.RWMutex
	ctx          context.Context

	metrics *dispatcherMetrics
	router  *router // nil: in-memory only
}

type peerChan struct {
	id    string
	token string // distinguishes registrations of the same peer ID
	since int64  // registration time, Unix milliseconds
	// ch carries messages from peers on this instance. It is unbuffered, so the sender
	// waits until the handler takes the message, as upstream does.
	ch chan *proto.EncryptedMessage
	// remote carries messages that arrived through Redis; full means drop.
	remote chan *proto.EncryptedMessage
	ctx    context.Context
	cancel context.CancelFunc

	// claimed and superseded are guarded by Dispatcher.mu.
	claimed    bool // this registration's key was written to Redis
	superseded bool // another instance now owns the key
}

// NewDispatcher creates a dispatcher configured from the environment (see README.md).
// Without NB_SIGNAL_REDIS_URL it is in-memory only. An unreachable Redis does not fail
// startup: the dispatcher runs local-only until Redis answers.
func NewDispatcher(ctx context.Context, meter metric.Meter) (*Dispatcher, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return New(ctx, meter, cfg)
}

// New creates a dispatcher with an explicit configuration. Background work stops when ctx
// is cancelled.
func New(ctx context.Context, meter metric.Meter, cfg Config) (*Dispatcher, error) {
	d := &Dispatcher{
		peerChannels: make(map[string]*peerChan),
		ctx:          ctx,
	}
	m, err := newDispatcherMetrics(meter, d)
	if err != nil {
		return nil, err
	}
	d.metrics = m
	if !cfg.enabled() {
		return d, nil
	}
	r, err := newRouter(ctx, d, cfg)
	if err != nil {
		return nil, err
	}
	d.router = r
	return d, nil
}

// InstanceID returns the ID this instance registers its peers under, or "" when Redis
// routing is off.
func (d *Dispatcher) InstanceID() string {
	if d.router == nil {
		return ""
	}
	return d.router.instanceID
}

// Degraded reports whether Redis routing is configured but currently unavailable, so only
// peers on this instance can reach each other.
func (d *Dispatcher) Degraded() bool {
	return d.router != nil && d.router.degraded.Load()
}

func newToken() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (d *Dispatcher) lookup(id string) (*peerChan, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok := d.peerChannels[id]
	return p, ok
}

// SendMessage delivers msg to msg.RemoteKey. A peer on this instance gets it directly; any
// other peer is looked up in Redis and the message is published to its instance's channel.
// Undeliverable messages are dropped and counted, never returned as errors, as upstream
// does for peers that are not connected.
func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage) (*proto.EncryptedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	default:
	}

	if msg.RemoteKey == "dummy" {
		// Test message send during netbird status
		return &proto.EncryptedMessage{}, nil
	}

	peerChan, ok := d.lookup(msg.RemoteKey)
	if ok && d.router != nil && d.isSuperseded(peerChan) {
		// The peer reconnected to another instance; this stream is stale.
		ok = false
	}
	if !ok {
		if d.router != nil {
			d.router.send(ctx, msg)
			return &proto.EncryptedMessage{}, nil
		}
		d.metrics.drop(ctx, dropNotConnected)
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	case <-peerChan.ctx.Done():
		// Channel was disconnected after we took it from the map
		d.metrics.drop(ctx, dropNotConnected)
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	case peerChan.ch <- msg:
		d.metrics.delivered(ctx, pathLocal)
		return &proto.EncryptedMessage{}, nil
	}
}

// deliverRemote hands a message that came through Redis to its local peer without blocking
// the subscription reader.
func (d *Dispatcher) deliverRemote(ctx context.Context, msg *proto.EncryptedMessage) {
	p, ok := d.lookup(msg.RemoteKey)
	if !ok {
		d.metrics.drop(ctx, dropNoLocalPeer)
		log.Tracef("message for peer [%s] arrived from Redis but the peer is no longer here", msg.RemoteKey)
		return
	}
	select {
	case <-p.ctx.Done():
		d.metrics.drop(ctx, dropNoLocalPeer)
	case p.remote <- msg:
		d.metrics.delivered(ctx, pathRedisIn)
	default:
		d.metrics.drop(ctx, dropQueueFull)
		log.Debugf("dropping message for peer [%s]: %d messages already queued", msg.RemoteKey, remoteQueueSize)
	}
}

// ListenForMessages registers id on this instance until ctx is done and runs messageHandler
// for every message addressed to it. A later registration of the same id replaces this one.
func (d *Dispatcher) ListenForMessages(ctx context.Context, id string, messageHandler func(context.Context, *proto.EncryptedMessage)) error {
	pctx, cancel := context.WithCancel(ctx)
	peerChan := &peerChan{
		id:     id,
		token:  newToken(),
		since:  time.Now().UnixMilli(),
		ch:     make(chan *proto.EncryptedMessage),
		remote: make(chan *proto.EncryptedMessage, remoteQueueSize),
		ctx:    pctx,
		cancel: cancel,
	}

	d.mu.Lock()
	d.peerChannels[id] = peerChan
	d.mu.Unlock()

	if d.router != nil {
		d.router.claim(peerChan)
	}

	go func() {
		defer func() {
			peerChan.cancel()
			d.mu.Lock()
			// Only remove our own registration: a newer stream for the same peer may
			// have replaced it already. The channels are never closed, so a sender that
			// took peerChan from the map before this point sees ctx.Done instead of a
			// panic.
			mine := d.peerChannels[id] == peerChan
			if mine {
				delete(d.peerChannels, id)
			}
			d.mu.Unlock()
			if mine && d.router != nil {
				d.router.release(peerChan)
			}
			log.Debugf("stream closed for peer %s", id)
		}()

		for {
			select {
			case <-pctx.Done():
				return
			case msg := <-peerChan.ch:
				if msg != nil {
					messageHandler(ctx, msg)
				}
			case msg := <-peerChan.remote:
				if msg != nil {
					messageHandler(ctx, msg)
				}
			}
		}
	}()

	return nil
}

// snapshot returns the current registrations.
func (d *Dispatcher) snapshot() []*peerChan {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*peerChan, 0, len(d.peerChannels))
	for _, p := range d.peerChannels {
		out = append(out, p)
	}
	return out
}

func (d *Dispatcher) isSuperseded(p *peerChan) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return p.superseded
}

// isCurrent reports whether p is still the registration for its peer ID.
func (d *Dispatcher) isCurrent(p *peerChan) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peerChannels[p.id] == p
}

func (d *Dispatcher) localPeers() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.peerChannels)
}
