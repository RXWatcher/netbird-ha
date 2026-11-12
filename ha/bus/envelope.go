// Package bus carries "account X changed" messages between management instances.
//
// A message never contains a network map or any peer data: it names the account, the
// peers and the reason, and every receiving instance recomputes from the shared
// database for the streams it holds itself. That keeps messages small and makes a
// lost or duplicated message harmless apart from latency (see the resync in
// ha/controller).
package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Version is the envelope schema version this build writes. Receivers accept any
// version >= 1: fields are only ever added and a kind's meaning never changes (a
// changed meaning gets a new kind). A receiver that sees a kind it does not know
// falls back to a full refresh of the named account, so releases N and N+1 can run
// side by side.
const Version = 1

// Kind names the network_map.Controller call that produced a message.
type Kind string

const (
	KindUpdateAccountPeers        Kind = "update_account_peers"
	KindBufferUpdateAccountPeers  Kind = "buffer_update_account_peers"
	KindUpdateAffectedPeers       Kind = "update_affected_peers"
	KindBufferUpdateAffectedPeers Kind = "buffer_update_affected_peers"
	KindUpdateAccountPeer         Kind = "update_account_peer"
	KindPeersAdded                Kind = "peers_added"
	KindPeersUpdated              Kind = "peers_updated"
	KindPeersDeleted              Kind = "peers_deleted"
	KindDisconnectPeers           Kind = "disconnect_peers"
	// KindResync asks every instance to resend the account's network maps. It is
	// published for accounts whose original message could not be published.
	KindResync Kind = "resync"
)

// Reason mirrors types.UpdateReason without importing upstream into the wire format.
type Reason struct {
	Resource  string `json:"resource,omitempty"`
	Operation string `json:"operation,omitempty"`
}

// Envelope is one bus message. The JSON names are the wire contract.
type Envelope struct {
	V       int      `json:"v"`
	Origin  string   `json:"origin"`
	Kind    Kind     `json:"kind"`
	Account string   `json:"account"`
	PeerIDs []string `json:"peerIDs,omitempty"`
	// Affected carries the affectedPeerIDs argument of the OnPeers* calls.
	Affected []string `json:"affected,omitempty"`
	Reason   *Reason  `json:"reason,omitempty"`
	// SentAt is informational (lag metrics, debugging); receivers never order by it.
	SentAt time.Time `json:"sentAt"`
}

func encode(e Envelope) ([]byte, error) {
	return json.Marshal(e)
}

var errInvalid = errors.New("invalid envelope")

func decode(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("%w: %v", errInvalid, err)
	}
	if e.V < 1 || e.Origin == "" || e.Kind == "" || e.Account == "" {
		return e, fmt.Errorf("%w: missing v, origin, kind or account", errInvalid)
	}
	return e, nil
}
