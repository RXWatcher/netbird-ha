package dispatcher

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	pathLocal    = "local"     // to a peer on this instance
	pathRedisOut = "redis_out" // published for a peer on another instance
	pathRedisIn  = "redis_in"  // arrived from another instance and queued for a local peer

	dropNotConnected     = "not_connected"     // no such peer here and Redis routing is off
	dropNotRegistered    = "not_registered"    // no peer key in Redis
	dropRedisUnavailable = "redis_unavailable" // Redis down: local-only mode
	dropNoSubscriber     = "no_subscriber"     // owner instance is not subscribed (dead or reconnecting)
	dropStaleOwner       = "stale_owner"       // the key names this instance but the peer is gone
	dropNoLocalPeer      = "no_local_peer"     // arrived from Redis after the peer left
	dropQueueFull        = "queue_full"        // the local peer's queue is full
	dropBadEnvelope      = "bad_envelope"      // undecodable or unknown envelope version
)

type dispatcherMetrics struct {
	messages     metric.Int64Counter
	dropped      metric.Int64Counter
	redisErrors  metric.Int64Counter
	ownerChanges metric.Int64Counter
}

func newDispatcherMetrics(meter metric.Meter, d *Dispatcher) (*dispatcherMetrics, error) {
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("")
	}
	m := &dispatcherMetrics{}
	var err error
	if m.messages, err = meter.Int64Counter("dispatcher_messages_total",
		metric.WithDescription("Messages handed to a local peer or published to another instance, by path")); err != nil {
		return nil, err
	}
	if m.dropped, err = meter.Int64Counter("dispatcher_dropped_messages_total",
		metric.WithDescription("Messages the dispatcher could not deliver, by reason")); err != nil {
		return nil, err
	}
	if m.redisErrors, err = meter.Int64Counter("dispatcher_redis_errors_total",
		metric.WithDescription("Failed Redis operations, by operation")); err != nil {
		return nil, err
	}
	if m.ownerChanges, err = meter.Int64Counter("dispatcher_peer_moved_total",
		metric.WithDescription("Local peers whose Redis key was taken over by another instance")); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("dispatcher_local_peers",
		metric.WithDescription("Peers registered on this instance"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(d.localPeers()))
			return nil
		})); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("dispatcher_redis_degraded",
		metric.WithDescription("1 while Redis routing is configured but unavailable (local-only mode)"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if d.Degraded() {
				o.Observe(1)
			} else {
				o.Observe(0)
			}
			return nil
		})); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *dispatcherMetrics) delivered(ctx context.Context, path string) {
	m.messages.Add(ctx, 1, metric.WithAttributes(attribute.String("path", path)))
}

func (m *dispatcherMetrics) drop(ctx context.Context, reason string) {
	m.dropped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *dispatcherMetrics) redisError(ctx context.Context, op string) {
	m.redisErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op)))
}
