package bus

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"
)

// Handler receives messages published by other instances. It is called from the
// subscriber goroutine, one message at a time, and must not block for long.
type Handler func(ctx context.Context, e Envelope)

// Bus is a broadcast channel shared by all management instances.
type Bus interface {
	// Origin is this instance's unique ID; messages carrying it are not delivered back.
	Origin() string
	// Publish fills V, Origin and SentAt and sends e to every other instance.
	Publish(ctx context.Context, e Envelope) error
	// Run subscribes and delivers messages to h until ctx is done. It reconnects and
	// resubscribes on its own and calls onSubscribed after every successful
	// (re)subscription, which is where callers resync: messages published while the
	// subscription was down are lost.
	Run(ctx context.Context, h Handler, onSubscribed func(ctx context.Context))
	Metrics() *Metrics
	Close() error
}

// Metrics are plain counters so tests can read them; Register exports them over OTel.
type Metrics struct {
	Published      atomic.Int64
	PublishErrors  atomic.Int64
	Received       atomic.Int64
	DroppedInvalid atomic.Int64
	Subscriptions  atomic.Int64
	ReceiveErrors  atomic.Int64
	Connected      atomic.Int64 // 1 while subscribed
}

// Register exports the counters on meter. Safe to call once per Metrics.
func (m *Metrics) Register(meter metric.Meter) error {
	counters := []struct {
		name, desc string
		v          *atomic.Int64
	}{
		{"netbird_ha_bus_published_total", "messages published", &m.Published},
		{"netbird_ha_bus_publish_errors_total", "failed publishes", &m.PublishErrors},
		{"netbird_ha_bus_received_total", "messages received from other instances", &m.Received},
		{"netbird_ha_bus_dropped_invalid_total", "undecodable messages dropped", &m.DroppedInvalid},
		{"netbird_ha_bus_subscriptions_total", "successful (re)subscriptions", &m.Subscriptions},
		{"netbird_ha_bus_receive_errors_total", "receive errors (each one reconnects)", &m.ReceiveErrors},
	}
	for _, c := range counters {
		v := c.v
		if _, err := meter.Int64ObservableCounter(c.name, metric.WithDescription(c.desc),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(v.Load())
				return nil
			})); err != nil {
			return err
		}
	}
	_, err := meter.Int64ObservableGauge("netbird_ha_bus_connected", metric.WithDescription("1 while the bus subscription is up"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.Connected.Load())
			return nil
		}))
	return err
}
