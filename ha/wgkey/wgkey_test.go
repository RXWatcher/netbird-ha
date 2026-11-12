package wgkey

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	nbgrpc "github.com/netbirdio/netbird/management/internals/shared/grpc"
)

type fakeInner struct {
	nbgrpc.SecretsManager
	key     wgtypes.Key
	refresh []string
}

func (f *fakeInner) GetWGKey() (wgtypes.Key, error) { return f.key, nil }
func (f *fakeInner) GenerateRelayToken() (*nbgrpc.Token, error) {
	return &nbgrpc.Token{Payload: "relay"}, nil
}
func (f *fakeInner) SetupRefresh(_ context.Context, accountID, peerKey string) {
	f.refresh = append(f.refresh, accountID+"/"+peerKey)
}

func TestFromEnv(t *testing.T) {
	_, ok, err := FromEnv()
	require.NoError(t, err)
	assert.False(t, ok)

	k, err := wgtypes.GeneratePrivateKey()
	require.NoError(t, err)

	t.Setenv(EnvKey, " "+k.String()+"\n")
	got, ok, err := FromEnv()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, k, got)

	k2, _ := wgtypes.GeneratePrivateKey()
	path := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(path, []byte(k2.String()+"\n"), 0o600))
	t.Setenv(EnvKeyFile, path)
	got, ok, err = FromEnv()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, k2, got, "the file wins over the variable")

	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))
	_, _, err = FromEnv()
	require.Error(t, err)

	t.Setenv(EnvKeyFile, filepath.Join(t.TempDir(), "missing"))
	_, _, err = FromEnv()
	require.Error(t, err)

	t.Setenv(EnvKeyFile, "")
	t.Setenv(EnvKey, wgtypes.Key{}.String())
	_, _, err = FromEnv()
	require.Error(t, err)
}

func TestSecretsManagerSharesKeyAndDelegates(t *testing.T) {
	shared, _ := wgtypes.GeneratePrivateKey()
	random, _ := wgtypes.GeneratePrivateKey()
	built := 0
	inner := &fakeInner{key: random}
	m := New(shared, func() nbgrpc.SecretsManager { built++; return inner })

	got, err := m.GetWGKey()
	require.NoError(t, err)
	assert.Equal(t, shared, got)
	assert.Zero(t, built, "GetWGKey must not build the inner manager")

	tok, err := m.GenerateRelayToken()
	require.NoError(t, err)
	assert.Equal(t, "relay", tok.Payload)
	m.SetupRefresh(context.Background(), "acc", "pk")
	assert.Equal(t, []string{"acc/pk"}, inner.refresh)
	assert.Equal(t, 1, built, "inner is built once, on first use")
}

// Two instances with the same configured key must present the same public key to clients.
func TestTwoInstancesAgree(t *testing.T) {
	shared, _ := wgtypes.GeneratePrivateKey()
	newInner := func() nbgrpc.SecretsManager {
		r, _ := wgtypes.GeneratePrivateKey()
		return &fakeInner{key: r}
	}
	a, _ := New(shared, newInner).GetWGKey()
	b, _ := New(shared, newInner).GetWGKey()
	assert.Equal(t, a.PublicKey(), b.PublicKey())
}
