package bus

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryHub is an in-process bus for tests: every Bus from the same hub sees the
// others' messages. Delivery is synchronous in the publisher's goroutine, which
// makes tests deterministic.
type MemoryHub struct {
	mu   sync.Mutex
	subs map[string]*memorySub
	fail bool
}

type memorySub struct {
	ctx          context.Context
	h            Handler
	onSubscribed func(context.Context)
}

func NewMemoryHub() *MemoryHub { return &MemoryHub{subs: map[string]*memorySub{}} }

// SetFailing makes every Publish fail (simulates Redis being down).
func (m *MemoryHub) SetFailing(fail bool) {
	m.mu.Lock()
	m.fail = fail
	m.mu.Unlock()
}

// Resubscribe calls every subscriber's onSubscribed, as after a reconnect.
func (m *MemoryHub) Resubscribe() {
	m.mu.Lock()
	subs := make([]*memorySub, 0, len(m.subs))
	for _, s := range m.subs {
		subs = append(subs, s)
	}
	m.mu.Unlock()
	for _, s := range subs {
		if s.onSubscribed != nil {
			s.onSubscribed(s.ctx)
		}
	}
}

// Bus returns the bus of one instance.
func (m *MemoryHub) Bus(origin string) *Memory { return &Memory{hub: m, origin: origin} }

// Memory is one instance's view of a MemoryHub.
type Memory struct {
	hub     *MemoryHub
	origin  string
	metrics Metrics
}

func (b *Memory) Origin() string    { return b.origin }
func (b *Memory) Metrics() *Metrics { return &b.metrics }
func (b *Memory) Close() error      { return nil }

func (b *Memory) Publish(_ context.Context, e Envelope) error {
	e.V = Version
	e.Origin = b.origin
	e.SentAt = time.Now().UTC()
	if err := b.hub.PublishRaw(e); err != nil {
		b.metrics.PublishErrors.Add(1)
		return err
	}
	b.metrics.Published.Add(1)
	return nil
}

// PublishRaw delivers e as is, without filling V or Origin (to test old/new versions).
func (m *MemoryHub) PublishRaw(e Envelope) error {
	m.mu.Lock()
	if m.fail {
		m.mu.Unlock()
		return errors.New("memory bus: failing")
	}
	targets := map[string]*memorySub{}
	for k, v := range m.subs {
		targets[k] = v
	}
	m.mu.Unlock()
	for origin, s := range targets {
		if origin != e.Origin {
			s.h(s.ctx, e)
		}
	}
	return nil
}

// Run registers the subscriber, reports one subscription and blocks until ctx is done.
func (b *Memory) Run(ctx context.Context, h Handler, onSubscribed func(context.Context)) {
	b.hub.mu.Lock()
	b.hub.subs[b.origin] = &memorySub{ctx: ctx, h: func(ctx context.Context, e Envelope) {
		b.metrics.Received.Add(1)
		h(ctx, e)
	}, onSubscribed: onSubscribed}
	b.hub.mu.Unlock()
	b.metrics.Subscriptions.Add(1)
	if onSubscribed != nil {
		onSubscribed(ctx)
	}
	<-ctx.Done()
	b.hub.mu.Lock()
	delete(b.hub.subs, b.origin)
	b.hub.mu.Unlock()
}
