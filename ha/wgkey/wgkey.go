// Package wgkey gives every management instance the same server WireGuard key.
//
// Upstream generates a random key per process (TimeBasedAuthSecretsManager). Clients
// fetch the server key and then open their Sync and Job streams separately, so behind
// a load balancer they can encrypt for one instance's key and land on another, which
// fails with "error while decrypting Sync request message". The fix is one key shared
// by all instances; this package loads it and overrides GetWGKey.
package wgkey

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	nbgrpc "github.com/netbirdio/netbird/management/internals/shared/grpc"
)

const (
	// EnvKey holds the base64 WireGuard private key (as printed by `wg genkey`).
	EnvKey = "NB_HA_WG_PRIVATE_KEY"
	// EnvKeyFile names a file holding the key, e.g. a mounted secret. It wins over EnvKey.
	EnvKeyFile = "NB_HA_WG_PRIVATE_KEY_FILE"
)

// FromEnv loads the shared key. ok is false when neither variable is set.
func FromEnv() (key wgtypes.Key, ok bool, err error) {
	raw := os.Getenv(EnvKey)
	source := EnvKey
	if path := os.Getenv(EnvKeyFile); path != "" {
		b, err := os.ReadFile(path) //nolint:gosec // operator-supplied secret path
		if err != nil {
			return key, false, fmt.Errorf("read %s: %w", EnvKeyFile, err)
		}
		raw, source = string(b), path
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return key, false, nil
	}
	key, err = wgtypes.ParseKey(raw)
	if err != nil {
		return key, false, fmt.Errorf("parse WireGuard key from %s: %w", source, err)
	}
	if key == (wgtypes.Key{}) {
		return key, false, errors.New("shared WireGuard key is all zeros")
	}
	return key, true, nil
}

// SecretsManager is upstream's secrets manager with GetWGKey replaced. The inner
// manager is built on first use, because server.Inject stores this value before the
// dependencies of upstream's constructor exist.
type SecretsManager struct {
	key      wgtypes.Key
	newInner func() nbgrpc.SecretsManager
	once     sync.Once
	inner    nbgrpc.SecretsManager
}

var _ nbgrpc.SecretsManager = (*SecretsManager)(nil)

// New wraps the secrets manager that newInner builds.
func New(key wgtypes.Key, newInner func() nbgrpc.SecretsManager) *SecretsManager {
	return &SecretsManager{key: key, newInner: newInner}
}

func (m *SecretsManager) get() nbgrpc.SecretsManager {
	m.once.Do(func() { m.inner = m.newInner() })
	return m.inner
}

// GetWGKey returns the shared key instead of the inner manager's random one.
func (m *SecretsManager) GetWGKey() (wgtypes.Key, error) { return m.key, nil }

func (m *SecretsManager) GenerateTurnToken() (*nbgrpc.Token, error) {
	return m.get().GenerateTurnToken()
}

func (m *SecretsManager) GenerateRelayToken() (*nbgrpc.Token, error) {
	return m.get().GenerateRelayToken()
}

func (m *SecretsManager) SetupRefresh(ctx context.Context, accountID, peerKey string) {
	m.get().SetupRefresh(ctx, accountID, peerKey)
}

func (m *SecretsManager) CancelRefresh(peerKey string) { m.get().CancelRefresh(peerKey) }
