package dispatcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

const waitFor = 10 * time.Second

// backend is a Redis the scenario tests run against: miniredis in unit tests, Valkey
// containers in integration tests.
type backend interface {
	// client returns a new client; the caller closes it.
	client(t *testing.T) *redis.Client
	// stop makes Redis unreachable; start brings it back without the old data.
	stop(t *testing.T)
	start(t *testing.T)
	// expire waits until a key whose owner stopped refreshing it is gone.
	expire(t *testing.T, key string)
	config() Config
}

type receiver struct {
	ch     chan *proto.EncryptedMessage
	cancel context.CancelFunc
}

func listen(t *testing.T, d *Dispatcher, id string) *receiver {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan *proto.EncryptedMessage, 4096)
	require.NoError(t, d.ListenForMessages(ctx, id, func(_ context.Context, m *proto.EncryptedMessage) {
		ch <- m
	}))
	return &receiver{ch: ch, cancel: cancel}
}

func msg(from, to, body string) *proto.EncryptedMessage {
	return &proto.EncryptedMessage{Key: from, RemoteKey: to, Body: []byte(body)}
}

func send(t *testing.T, d *Dispatcher, m *proto.EncryptedMessage) {
	t.Helper()
	_, err := d.SendMessage(context.Background(), m)
	require.NoError(t, err)
}

func expectBody(t *testing.T, r *receiver, body string) {
	t.Helper()
	select {
	case m := <-r.ch:
		require.Equal(t, body, string(m.Body))
	case <-time.After(waitFor):
		t.Fatalf("no message %q within %s", body, waitFor)
	}
}

func expectNothing(t *testing.T, r *receiver, d time.Duration) {
	t.Helper()
	select {
	case m := <-r.ch:
		t.Fatalf("unexpected message %q", m.Body)
	case <-time.After(d):
	}
}

// sendUntilReceived resends until r gets a message, for windows in which messages may be
// dropped (failover, recovery). Returns once one arrives.
func sendUntilReceived(t *testing.T, d *Dispatcher, from, to string, r *receiver, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for i := 0; time.Now().Before(deadline); i++ {
		send(t, d, msg(from, to, fmt.Sprintf("retry-%d", i)))
		select {
		case <-r.ch:
			// Drain duplicates of earlier attempts that were still in flight.
			for {
				select {
				case <-r.ch:
				case <-time.After(200 * time.Millisecond):
					return
				}
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("message from %s to %s not delivered within %s", from, to, within)
}

// instance starts a dispatcher with its own client to b. kill closes the client without
// any cleanup, as a crashed process would.
type instance struct {
	*Dispatcher
	rdb    *redis.Client
	cancel context.CancelFunc
}

func (i *instance) kill() {
	_ = i.rdb.Close()
	i.cancel()
}

func newInstance(t *testing.T, b backend, id string) *instance {
	t.Helper()
	cfg := b.config()
	cfg.InstanceID = id
	cfg.Client = b.client(t)
	ctx, cancel := context.WithCancel(context.Background())
	d, err := New(ctx, nil, cfg)
	require.NoError(t, err)
	inst := &instance{Dispatcher: d, rdb: cfg.Client, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		_ = cfg.Client.Close()
	})
	waitSubscribed(t, inst)
	return inst
}

func waitSubscribed(t *testing.T, i *instance) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		n, err := i.rdb.Publish(ctx, i.router.channel, []byte{envelopeControl}).Result()
		return err == nil && n > 0
	}, waitFor, 20*time.Millisecond, "instance %s never subscribed", i.InstanceID())
}

func keyOwner(t *testing.T, rdb *redis.Client, d *Dispatcher, peer string) string {
	t.Helper()
	v, err := rdb.Get(context.Background(), d.router.peerKey(peer)).Result()
	if err == redis.Nil {
		return ""
	}
	require.NoError(t, err)
	for i := range v {
		if v[i] == '|' {
			return v[:i]
		}
	}
	return v
}

func waitOwner(t *testing.T, rdb *redis.Client, d *Dispatcher, peer, owner string) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := rdb.Get(ctx, d.router.peerKey(peer)).Result()
		if owner == "" {
			return err == redis.Nil
		}
		return err == nil && len(v) > len(owner) && v[:len(owner)+1] == owner+"|"
	}, waitFor, 20*time.Millisecond, "key of %s never owned by %q", peer, owner)
}
