package dispatcher

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	gproto "google.golang.org/protobuf/proto"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

// Redis layout (prefix defaults to "nbha:signal:"):
//
//	<prefix>peer:<peerID>    = "<instanceID>|<token>|<registeredUnixMs>", PX = peer TTL
//	<prefix>inst:<instanceID> pub/sub channel of one instance
//
// Envelope on an instance channel: one version byte, then the payload.
//
//	0x00 control ping (self-check), no payload
//	0x01 a proto-marshalled EncryptedMessage; the destination is its RemoteKey
const (
	envelopeControl byte = 0
	envelopeMessage byte = 1

	refreshBatch = 500
)

// sendScript looks up the owner of KEYS[1] and publishes ARGV[3] on its channel in one round
// trip. When nobody is subscribed to that channel the owner is gone, so the key is deleted.
// Returns {0} not registered, {1, value} owned by this instance, {2, value, receivers}.
var sendScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return {0} end
local inst = string.match(v, '^([^|]*)')
if inst == ARGV[2] then return {1, v} end
local n = redis.call('PUBLISH', ARGV[1] .. inst, ARGV[3])
if n == 0 then redis.call('DEL', KEYS[1]) end
return {2, v, n}
`)

// refreshScript extends or (re)writes this instance's peer keys.
// ARGV[1] = TTL in ms, ARGV[2] = "<instanceID>|"; for key i: ARGV[1+2i] = value,
// ARGV[2+2i] = "c" (claimed before) or "u" (never written, registered while degraded).
// Per key: 1 extended, 2 written, 0 another instance holds a newer registration.
var refreshScript = redis.NewScript(`
local out = {}
local mine = ARGV[2]
for i, k in ipairs(KEYS) do
  local val = ARGV[1 + 2 * i]
  local mode = ARGV[2 + 2 * i]
  local cur = redis.call('GET', k)
  if cur == val then
    redis.call('PEXPIRE', k, ARGV[1])
    out[i] = 1
  elseif (not cur) or string.sub(cur, 1, #mine) == mine then
    redis.call('SET', k, val, 'PX', ARGV[1])
    out[i] = 2
  elseif mode == 'u' and tonumber(string.match(cur, '|(%d+)$') or '0') < tonumber(string.match(val, '|(%d+)$')) then
    redis.call('SET', k, val, 'PX', ARGV[1])
    out[i] = 2
  else
    out[i] = 0
  end
end
return out
`)

// releaseScript deletes KEYS[i] only while it still holds ARGV[i]: a peer that moved to
// another instance, or re-registered here, keeps its newer key.
var releaseScript = redis.NewScript(`
local n = 0
for i, k in ipairs(KEYS) do
  if redis.call('GET', k) == ARGV[i] then
    redis.call('DEL', k)
    n = n + 1
  end
end
return n
`)

type router struct {
	d          *Dispatcher
	rdb        *redis.Client
	ownsClient bool
	cfg        Config
	instanceID string
	channel    string
	ctx        context.Context

	degraded    atomic.Bool
	subscribed  atomic.Bool // the current subscription has been confirmed
	reclaim     chan struct{}
	resubscribe chan struct{}
	done        chan struct{}
}

func newRouter(ctx context.Context, d *Dispatcher, cfg Config) (*router, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	r := &router{
		d:           d,
		rdb:         cfg.Client,
		cfg:         cfg,
		instanceID:  cfg.InstanceID,
		channel:     cfg.KeyPrefix + "inst:" + cfg.InstanceID,
		ctx:         ctx,
		reclaim:     make(chan struct{}, 1),
		resubscribe: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	if r.rdb == nil {
		if r.rdb, err = NewRedisClient(cfg.RedisURL); err != nil {
			return nil, err
		}
		r.ownsClient = true
	}

	pctx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
	err = r.rdb.Ping(pctx).Err()
	cancel()
	if err != nil {
		r.setDegraded("ping", err)
	}

	go r.subscribeLoop()
	go r.maintainLoop()

	log.Infof("signal dispatcher: Redis routing on, instance %s, peer TTL %s, heartbeat %s", r.instanceID, cfg.PeerTTL, cfg.Heartbeat)
	return r, nil
}

func (r *router) peerKey(id string) string { return r.cfg.KeyPrefix + "peer:" + id }

func (r *router) value(p *peerChan) string {
	return r.instanceID + "|" + p.token + "|" + strconv.FormatInt(p.since, 10)
}

func (r *router) setDegraded(op string, err error) {
	r.d.metrics.redisError(r.ctx, op)
	if r.degraded.CompareAndSwap(false, true) {
		log.Warnf("signal dispatcher: Redis unavailable (%s: %v); only peers on this instance can reach each other until it is back", op, err)
	}
}

func (r *router) setHealthy() {
	if r.degraded.CompareAndSwap(true, false) {
		log.Infof("signal dispatcher: Redis is back; routing between instances again")
		r.triggerReclaim()
	}
}

func (r *router) triggerReclaim() {
	select {
	case r.reclaim <- struct{}{}:
	default:
	}
}

// failed records a Redis error unless it came from callerCtx ending or from shutdown.
// Internal operations pass r.ctx, so their own timeouts do count.
func (r *router) failed(callerCtx context.Context, op string, err error) {
	if callerCtx.Err() != nil && errors.Is(err, callerCtx.Err()) || errors.Is(err, redis.ErrClosed) {
		return
	}
	r.setDegraded(op, err)
}

// claim writes the key of a new registration. The newest stream wins, so the write is
// unconditional; a peer that moved here takes the key from its old instance.
func (r *router) claim(p *peerChan) {
	if r.degraded.Load() {
		// Written by the next refresh after Redis is back.
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpTimeout)
	defer cancel()
	if err := r.rdb.Set(ctx, r.peerKey(p.id), r.value(p), r.cfg.PeerTTL).Err(); err != nil {
		r.failed(r.ctx, "claim", err)
		return
	}
	r.d.mu.Lock()
	p.claimed = true
	r.d.mu.Unlock()
}

// release deletes the key of an ended registration if it is still this registration's.
func (r *router) release(p *peerChan) {
	r.d.mu.RLock()
	claimed := p.claimed
	r.d.mu.RUnlock()
	if !claimed || r.degraded.Load() {
		return // never written, or the TTL removes it
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), r.cfg.OpTimeout)
		defer cancel()
		if err := releaseScript.Run(ctx, r.rdb, []string{r.peerKey(p.id)}, r.value(p)).Err(); err != nil {
			r.failed(r.ctx, "release", err)
		}
	}()
}

// send routes msg to the instance that owns msg.RemoteKey.
func (r *router) send(ctx context.Context, msg *proto.EncryptedMessage) {
	if r.degraded.Load() {
		r.d.metrics.drop(ctx, dropRedisUnavailable)
		log.Tracef("Redis unavailable: dropping message from peer [%s] to peer [%s]", msg.Key, msg.RemoteKey)
		return
	}
	body, err := gproto.Marshal(msg)
	if err != nil {
		r.d.metrics.drop(ctx, dropBadEnvelope)
		return
	}
	payload := make([]byte, 0, len(body)+1)
	payload = append(payload, envelopeMessage)
	payload = append(payload, body...)

	octx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	res, err := sendScript.Run(octx, r.rdb, []string{r.peerKey(msg.RemoteKey)},
		r.cfg.KeyPrefix+"inst:", r.instanceID, payload).Slice()
	if err != nil {
		r.failed(ctx, "send", err)
		r.d.metrics.drop(ctx, dropRedisUnavailable)
		return
	}
	switch status, _ := res[0].(int64); status {
	case 0:
		r.d.metrics.drop(ctx, dropNotRegistered)
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected to any instance", msg.Key, msg.RemoteKey)
	case 1:
		r.d.metrics.drop(ctx, dropStaleOwner)
	case 2:
		if n, _ := res[2].(int64); n == 0 {
			r.d.metrics.drop(ctx, dropNoSubscriber)
			log.Tracef("instance %v of peer [%s] is not subscribed; dropped its stale key", res[1], msg.RemoteKey)
			return
		}
		r.d.metrics.delivered(ctx, pathRedisOut)
	}
}

func (r *router) handle(payload string) {
	if len(payload) == 0 {
		r.d.metrics.drop(r.ctx, dropBadEnvelope)
		return
	}
	switch payload[0] {
	case envelopeControl:
	case envelopeMessage:
		msg := &proto.EncryptedMessage{}
		if err := gproto.Unmarshal([]byte(payload[1:]), msg); err != nil {
			r.d.metrics.drop(r.ctx, dropBadEnvelope)
			log.Debugf("signal dispatcher: undecodable message on %s: %v", r.channel, err)
			return
		}
		r.d.deliverRemote(r.ctx, msg)
	default:
		r.d.metrics.drop(r.ctx, dropBadEnvelope)
		log.Debugf("signal dispatcher: unknown envelope version %d on %s", payload[0], r.channel)
	}
}

// subscribeLoop keeps this instance subscribed to its channel. go-redis reconnects and
// resubscribes by itself; a self-check failure forces a fresh subscription.
func (r *router) subscribeLoop() {
	for {
		ps := r.rdb.Subscribe(r.ctx, r.channel)
		ch := ps.ChannelWithSubscriptions(redis.WithChannelSize(1024))
		restart := false
		for !restart {
			select {
			case <-r.ctx.Done():
				_ = ps.Close()
				r.shutdown()
				return
			case <-r.resubscribe:
				restart = true
			case m, ok := <-ch:
				if !ok {
					restart = true
					break
				}
				switch m := m.(type) {
				case *redis.Subscription:
					if m.Kind == "subscribe" {
						log.Debugf("signal dispatcher: subscribed to %s", r.channel)
						// A self-check queued before this confirmation is stale.
						select {
						case <-r.resubscribe:
						default:
						}
						r.subscribed.Store(true)
						r.triggerReclaim()
					}
				case *redis.Message:
					r.handle(m.Payload)
				}
			}
		}
		r.subscribed.Store(false)
		_ = ps.Close()
		select {
		case <-r.ctx.Done():
			r.shutdown()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *router) maintainLoop() {
	heartbeat := time.NewTicker(r.cfg.Heartbeat)
	selfCheck := time.NewTicker(r.cfg.SelfCheckInterval)
	probe := time.NewTicker(r.cfg.ProbeInterval)
	defer heartbeat.Stop()
	defer selfCheck.Stop()
	defer probe.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.reclaim:
			if !r.degraded.Load() {
				r.refreshAll()
			}
		case <-heartbeat.C:
			if !r.degraded.Load() {
				r.refreshAll()
			}
		case <-selfCheck.C:
			if !r.degraded.Load() {
				r.selfCheck()
			}
		case <-probe.C:
			if r.degraded.Load() {
				ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpTimeout)
				err := r.rdb.Ping(ctx).Err()
				cancel()
				if err == nil {
					r.setHealthy()
				}
			}
		}
	}
}

// selfCheck publishes a ping on this instance's channel. PUBLISH counts only subscribers on
// the node that executes it, so zero means the subscription is gone or sits on a node that is
// no longer the master (after a Sentinel failover that left the old master running).
func (r *router) selfCheck() {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpTimeout)
	defer cancel()
	n, err := r.rdb.Publish(ctx, r.channel, []byte{envelopeControl}).Result()
	if err != nil {
		r.failed(r.ctx, "self_check", err)
		return
	}
	if n == 0 && r.subscribed.Load() {
		log.Warnf("signal dispatcher: no subscriber on %s; subscribing again", r.channel)
		select {
		case r.resubscribe <- struct{}{}:
		default:
		}
	}
}

// refreshAll extends the keys of every local registration, rewrites keys lost in Redis (for
// example after a failover), and marks registrations whose peer moved to another instance.
func (r *router) refreshAll() {
	var peers []*peerChan
	modes := map[*peerChan]string{}
	r.d.mu.RLock()
	for _, p := range r.d.peerChannels {
		if p.superseded {
			continue
		}
		peers = append(peers, p)
		if p.claimed {
			modes[p] = "c"
		} else {
			modes[p] = "u"
		}
	}
	r.d.mu.RUnlock()

	for start := 0; start < len(peers); start += refreshBatch {
		batch := peers[start:min(start+refreshBatch, len(peers))]
		keys := make([]string, len(batch))
		args := make([]interface{}, 0, 2+2*len(batch))
		args = append(args, r.cfg.PeerTTL.Milliseconds(), r.instanceID+"|")
		for i, p := range batch {
			keys[i] = r.peerKey(p.id)
			args = append(args, r.value(p), modes[p])
		}
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpTimeout+time.Duration(len(batch))*time.Millisecond)
		res, err := refreshScript.Run(ctx, r.rdb, keys, args...).Int64Slice()
		cancel()
		if err != nil {
			r.failed(r.ctx, "refresh", err)
			return
		}
		for i, status := range res {
			p := batch[i]
			switch status {
			case 1, 2:
				r.d.mu.Lock()
				p.claimed = true
				r.d.mu.Unlock()
				if status == 2 && !r.d.isCurrent(p) {
					// The stream ended while the batch ran; undo the write.
					r.release(p)
				}
			case 0:
				r.d.mu.Lock()
				p.superseded = true
				r.d.mu.Unlock()
				r.d.metrics.ownerChanges.Add(r.ctx, 1)
				log.Debugf("signal dispatcher: peer [%s] is now registered on another instance", p.id)
			}
		}
	}
}

// shutdown deletes the keys this instance still owns so other instances stop routing here
// before the TTL runs out.
func (r *router) shutdown() {
	select {
	case <-r.done:
		return
	default:
		close(r.done)
	}
	var keys []string
	var vals []interface{}
	r.d.mu.RLock()
	for _, p := range r.d.peerChannels {
		if p.claimed && !p.superseded {
			keys = append(keys, r.peerKey(p.id))
			vals = append(vals, r.value(p))
		}
	}
	r.d.mu.RUnlock()
	if len(keys) > 0 && !r.degraded.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := releaseScript.Run(ctx, r.rdb, keys, vals...).Err(); err != nil {
			log.Debugf("signal dispatcher: releasing %d peer keys at shutdown: %v", len(keys), err)
		}
		cancel()
	}
	if r.ownsClient {
		if err := r.rdb.Close(); err != nil {
			log.Debugf("signal dispatcher: closing Redis client: %v", err)
		}
	}
	log.Debugf("signal dispatcher: instance %s stopped", r.instanceID)
}
