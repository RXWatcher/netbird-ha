package dispatcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The scenarios run against every backend: miniredis (redis_test.go) and real Valkey
// containers (integration_test.go).

func scenarioCrossInstance(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	ra := listen(t, a.Dispatcher, "peer-a")
	rc := listen(t, c.Dispatcher, "peer-c")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-c", "c")

	send(t, c.Dispatcher, msg("peer-c", "peer-a", "c->a"))
	expectBody(t, ra, "c->a")
	send(t, a.Dispatcher, msg("peer-a", "peer-c", "a->c"))
	expectBody(t, rc, "a->c")

	// Local fast path still works with Redis on.
	rl := listen(t, a.Dispatcher, "peer-a2")
	send(t, a.Dispatcher, msg("peer-a", "peer-a2", "local"))
	expectBody(t, rl, "local")

	// Unknown peers are dropped without an error.
	send(t, a.Dispatcher, msg("peer-a", "nobody", "lost"))
}

func scenarioOrdering(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	ra := listen(t, a.Dispatcher, "peer-a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")

	const n = 300
	for i := 0; i < n; i++ {
		send(t, c.Dispatcher, msg("peer-c", "peer-a", fmt.Sprint(i)))
	}
	for i := 0; i < n; i++ {
		expectBody(t, ra, fmt.Sprint(i))
	}
}

func scenarioPeerMoves(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	bb := newInstance(t, b, "b")
	c := newInstance(t, b, "c")

	oldStream := listen(t, a.Dispatcher, "mover")
	waitOwner(t, a.rdb, a.Dispatcher, "mover", "a")
	send(t, c.Dispatcher, msg("peer-c", "mover", "before"))
	expectBody(t, oldStream, "before")

	// The peer reconnects to b while its old stream on a is still open.
	newStream := listen(t, bb.Dispatcher, "mover")
	waitOwner(t, a.rdb, a.Dispatcher, "mover", "b")
	send(t, c.Dispatcher, msg("peer-c", "mover", "after"))
	expectBody(t, newStream, "after")
	expectNothing(t, oldStream, 100*time.Millisecond)

	// a's heartbeat notices the move and stops treating its stream as the peer's.
	a.router.refreshAll()
	require.True(t, a.isSuperseded(mustLookup(t, a.Dispatcher, "mover")))
	send(t, a.Dispatcher, msg("peer-a", "mover", "from-a"))
	expectBody(t, newStream, "from-a")
	expectNothing(t, oldStream, 100*time.Millisecond)

	// The old stream ends: a must not delete b's key.
	oldStream.cancel()
	require.Eventually(t, func() bool { _, ok := a.lookup("mover"); return !ok }, waitFor, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond) // release runs asynchronously; give it the chance to misbehave
	require.Equal(t, "b", keyOwner(t, a.rdb, a.Dispatcher, "mover"))
	send(t, c.Dispatcher, msg("peer-c", "mover", "still-b"))
	expectBody(t, newStream, "still-b")

	// And a heartbeat on a never takes it back.
	a.router.refreshAll()
	require.Equal(t, "b", keyOwner(t, a.rdb, a.Dispatcher, "mover"))
}

func scenarioReregisterSameInstance(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	first := listen(t, a.Dispatcher, "p")
	waitOwner(t, a.rdb, a.Dispatcher, "p", "a")
	firstValue := a.router.value(mustLookup(t, a.Dispatcher, "p"))

	second := listen(t, a.Dispatcher, "p")
	secondValue := a.router.value(mustLookup(t, a.Dispatcher, "p"))
	require.NotEqual(t, firstValue, secondValue)

	first.cancel()
	time.Sleep(200 * time.Millisecond)
	v, err := a.rdb.Get(context.Background(), a.router.peerKey("p")).Result()
	require.NoError(t, err)
	require.Equal(t, secondValue, v, "ending the old stream must keep the new registration's key")
	send(t, c.Dispatcher, msg("peer-c", "p", "to-second"))
	expectBody(t, second, "to-second")

	second.cancel()
	waitOwner(t, a.rdb, a.Dispatcher, "p", "")
}

func scenarioInstanceKilled(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	listen(t, a.Dispatcher, "orphan")
	waitOwner(t, c.rdb, c.Dispatcher, "orphan", "a")

	a.kill()
	// Nobody refreshes the key any more: it expires with its TTL.
	b.expire(t, c.router.peerKey("orphan"))
	require.Equal(t, "", keyOwner(t, c.rdb, c.Dispatcher, "orphan"))

	// The peer reconnects to c and is reachable again.
	r := listen(t, c.Dispatcher, "orphan")
	d := newInstance(t, b, "d")
	send(t, d.Dispatcher, msg("peer-d", "orphan", "back"))
	expectBody(t, r, "back")
}

func scenarioKilledOwnerCleanedOnSend(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	listen(t, a.Dispatcher, "orphan")
	waitOwner(t, c.rdb, c.Dispatcher, "orphan", "a")

	a.kill()
	// The first message to a dead instance finds no subscriber and removes the stale key
	// instead of waiting for the TTL.
	require.Eventually(t, func() bool {
		send(t, c.Dispatcher, msg("peer-c", "orphan", "x"))
		return keyOwner(t, c.rdb, c.Dispatcher, "orphan") == ""
	}, waitFor, 50*time.Millisecond)
}

func scenarioRedisDown(t *testing.T, b backend) {
	a := newInstance(t, b, "a")
	c := newInstance(t, b, "c")
	ra := listen(t, a.Dispatcher, "peer-a")
	rc := listen(t, c.Dispatcher, "peer-c")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-c", "c")

	b.stop(t)
	require.Eventually(t, func() bool {
		send(t, c.Dispatcher, msg("peer-c", "peer-a", "probe"))
		return c.Degraded()
	}, waitFor, 20*time.Millisecond)

	// Local-only: peers on the same instance still reach each other, new peers can
	// register, and cross-instance messages are dropped without errors.
	rc2 := listen(t, c.Dispatcher, "peer-c2")
	send(t, c.Dispatcher, msg("peer-c", "peer-c2", "local-while-down"))
	expectBody(t, rc2, "local-while-down")
	send(t, c.Dispatcher, msg("peer-c2", "peer-c", "local-back"))
	expectBody(t, rc, "local-back")
	drainAll(ra)

	b.start(t)
	require.Eventually(t, func() bool { return !a.Degraded() && !c.Degraded() }, waitFor, 20*time.Millisecond)
	// Redis came back empty: every instance rewrites its peers' keys, including the one
	// registered during the outage.
	waitOwner(t, a.rdb, a.Dispatcher, "peer-a", "a")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-c", "c")
	waitOwner(t, a.rdb, a.Dispatcher, "peer-c2", "c")
	sendUntilReceived(t, c.Dispatcher, "peer-c", "peer-a", ra, waitFor)
	sendUntilReceived(t, a.Dispatcher, "peer-a", "peer-c2", rc2, waitFor)
}

func drainAll(r *receiver) {
	for {
		select {
		case <-r.ch:
		default:
			return
		}
	}
}

func mustLookup(t *testing.T, d *Dispatcher, id string) *peerChan {
	t.Helper()
	p, ok := d.lookup(id)
	require.True(t, ok, "peer %s not registered", id)
	return p
}

var scenarios = []struct {
	name string
	run  func(*testing.T, backend)
}{
	{"CrossInstance", scenarioCrossInstance},
	{"Ordering", scenarioOrdering},
	{"PeerMoves", scenarioPeerMoves},
	{"ReregisterSameInstance", scenarioReregisterSameInstance},
	{"InstanceKilled", scenarioInstanceKilled},
	{"KilledOwnerCleanedOnSend", scenarioKilledOwnerCleanedOnSend},
	{"RedisDown", scenarioRedisDown},
}
