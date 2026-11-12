package dispatcher_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/netbirdio/netbird/shared/signal/proto"
	"github.com/netbirdio/netbird/signal/server"

	"github.com/netbirdio/signal-dispatcher/dispatcher"
)

// These tests run NetBird's own signal server, unchanged, on top of this module: two
// instances share one Redis, and clients talk to them over gRPC as stock clients do.

func startSignal(t *testing.T, redisURL, instanceID string) proto.SignalExchangeClient {
	t.Helper()
	t.Setenv(dispatcher.EnvRedisURL, redisURL)
	t.Setenv(dispatcher.EnvSignalInstanceID, instanceID)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv, err := server.NewServer(ctx, otel.Meter(""))
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer()
	proto.RegisterSignalExchangeServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewSignalExchangeClient(conn)
}

type stream struct {
	proto.SignalExchange_ConnectStreamClient
	cancel context.CancelFunc
	msgs   chan *proto.EncryptedMessage
}

func connect(t *testing.T, c proto.SignalExchangeClient, id string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.Background(), proto.HeaderId, id))
	t.Cleanup(cancel)
	s, err := c.ConnectStream(ctx)
	require.NoError(t, err)
	// The server sends its header once the peer is registered.
	h, err := s.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"1"}, h.Get(proto.HeaderRegistered))
	st := &stream{SignalExchange_ConnectStreamClient: s, cancel: cancel, msgs: make(chan *proto.EncryptedMessage, 100)}
	go func() {
		for {
			m, err := s.Recv()
			if err != nil {
				return
			}
			st.msgs <- m
		}
	}()
	return st
}

func (s *stream) expect(t *testing.T, body string) {
	t.Helper()
	select {
	case m := <-s.msgs:
		require.Equal(t, body, string(m.Body))
	case <-time.After(5 * time.Second):
		t.Fatalf("no message %q", body)
	}
}

func sendVia(t *testing.T, c proto.SignalExchangeClient, from, to, body string) {
	t.Helper()
	_, err := c.Send(context.Background(), &proto.EncryptedMessage{Key: from, RemoteKey: to, Body: []byte(body)})
	require.NoError(t, err)
}

func waitKey(t *testing.T, mr *miniredis.Miniredis, peer, owner string) {
	t.Helper()
	require.Eventually(t, func() bool {
		v, err := mr.Get("nbha:signal:peer:" + peer)
		return err == nil && len(v) > len(owner) && v[:len(owner)+1] == owner+"|"
	}, 5*time.Second, 10*time.Millisecond)
}

func TestUpstreamSignalServersShareRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr()
	sigA := startSignal(t, url, "sig-a")
	sigB := startSignal(t, url, "sig-b")

	alice := connect(t, sigA, "alice")
	bob := connect(t, sigB, "bob")
	waitKey(t, mr, "alice", "sig-a")
	waitKey(t, mr, "bob", "sig-b")

	sendVia(t, sigA, "alice", "bob", "offer")
	bob.expect(t, "offer")
	sendVia(t, sigB, "bob", "alice", "answer")
	alice.expect(t, "answer")

	// Bob reconnects to sig-a: messages sent through sig-b now cross back.
	bob.cancel()
	bob2 := connect(t, sigA, "bob")
	waitKey(t, mr, "bob", "sig-a")
	carol := connect(t, sigB, "carol")
	sendVia(t, sigB, "carol", "bob", "after-move")
	bob2.expect(t, "after-move")

	// Sig-b's cleanup of bob's old stream must not remove bob's new key.
	time.Sleep(200 * time.Millisecond)
	waitKey(t, mr, "bob", "sig-a")
	sendVia(t, sigA, "alice", "carol", "to-carol")
	carol.expect(t, "to-carol")
}
