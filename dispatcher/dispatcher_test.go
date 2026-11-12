package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

// In-memory mode: no NB_SIGNAL_REDIS_URL, the behavior of upstream's dispatcher.

func newMemory(t *testing.T) *Dispatcher {
	t.Helper()
	t.Setenv(EnvRedisURL, "")
	d, err := NewDispatcher(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, d.router)
	require.False(t, d.Degraded())
	return d
}

func TestMemoryDelivers(t *testing.T) {
	d := newMemory(t)
	r := listen(t, d, "p")
	send(t, d, msg("q", "p", "hi"))
	expectBody(t, r, "hi")
}

func TestMemoryDummyAndUnknownPeers(t *testing.T) {
	d := newMemory(t)
	for _, to := range []string{"dummy", "nobody"} {
		resp, err := d.SendMessage(context.Background(), msg("q", to, "x"))
		require.NoError(t, err)
		require.NotNil(t, resp)
	}
}

func TestMemoryCancelledSender(t *testing.T) {
	d := newMemory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.SendMessage(ctx, msg("q", "p", "x"))
	require.Error(t, err)
}

func TestMemorySendBlocksUntilHandled(t *testing.T) {
	d := newMemory(t)
	release := make(chan struct{})
	handled := make(chan string, 2)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, d.ListenForMessages(ctx, "p", func(_ context.Context, m *proto.EncryptedMessage) {
		<-release
		handled <- string(m.Body)
	}))
	send(t, d, msg("q", "p", "1")) // taken by the handler, which now blocks
	second := make(chan struct{})
	go func() {
		send(t, d, msg("q", "p", "2"))
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("second send returned while the handler was busy")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-second
	require.Equal(t, "1", <-handled)
	require.Equal(t, "2", <-handled)
}

func TestMemoryReregistrationKeepsNewest(t *testing.T) {
	d := newMemory(t)
	first := listen(t, d, "p")
	second := listen(t, d, "p")
	first.cancel()
	time.Sleep(50 * time.Millisecond)
	send(t, d, msg("q", "p", "to-second"))
	expectBody(t, second, "to-second")
	expectNothing(t, first, 50*time.Millisecond)
}

func TestMemoryDeregisters(t *testing.T) {
	d := newMemory(t)
	r := listen(t, d, "p")
	r.cancel()
	require.Eventually(t, func() bool { return d.localPeers() == 0 }, waitFor, 5*time.Millisecond)
	resp, err := d.SendMessage(context.Background(), msg("q", "p", "x"))
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestMemoryConcurrentSendAndDisconnect(t *testing.T) {
	// Senders racing a disconnect must neither panic nor block.
	d := newMemory(t)
	for i := 0; i < 200; i++ {
		r := listen(t, d, "p")
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = d.SendMessage(context.Background(), msg("q", "p", "x"))
		}()
		r.cancel()
		select {
		case <-done:
		case <-time.After(waitFor):
			t.Fatal("send blocked on a disconnected peer")
		}
	}
}
