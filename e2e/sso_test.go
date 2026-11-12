package e2e

import (
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/Bloem-Studios/netbird-ha/e2e/bed"
)

var deviceURLRe = regexp.MustCompile(`(http://lb(?::80)?/oauth2/device\?user_code=[A-Z0-9-]+)`)

// ssoClient starts a client without a setup key, signs it in as a local
// embedded-IdP user through the device flow (what a headless client uses) and
// waits until its peer is connected. The peer then belongs to that user.
func ssoClient(s *S, name, mgmtURL, email, password string) (*bed.DynClient, error) {
	ctx := s.Ctx
	dc, err := bed.StartDynClient(ctx, name, bed.DynClientOptions{ManagementURL: mgmtURL})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*bed.DynClient, error) {
		logs, _ := bed.Logs(ctx, dc.Container, time.Now().Add(-5*time.Minute))
		dc.Remove(ctx)
		if len(logs) > 2000 {
			logs = logs[len(logs)-2000:]
		}
		return nil, fmt.Errorf("%w\n%s log tail:\n%s", err, name, logs)
	}
	var deviceURL string
	if _, err := bed.Eventually(ctx, 60*time.Second, 250*time.Millisecond, func() error {
		out, err := bed.Logs(ctx, dc.Container, time.Now().Add(-5*time.Minute))
		if err != nil {
			return err
		}
		if m := deviceURLRe.FindStringSubmatch(out); m != nil {
			deviceURL = m[1]
			return nil
		}
		return fmt.Errorf("no device login URL in %s's log yet", name)
	}); err != nil {
		return fail(err)
	}
	u, err := url.Parse(s.B.API.Base)
	if err != nil {
		return fail(err)
	}
	if err := bed.DexDeviceLogin(ctx, u.Host, deviceURL, email, password); err != nil {
		return fail(fmt.Errorf("device login: %w", err))
	}
	// The client polls the token endpoint (every 5-8 s) and then logs in.
	if _, err := bed.Eventually(ctx, 60*time.Second, time.Second, func() error {
		p, err := s.B.API.PeerByName(ctx, name)
		if err != nil {
			return err
		}
		if !p.Connected || p.UserID == "" {
			return fmt.Errorf("peer %s connected=%v user=%q", name, p.Connected, p.UserID)
		}
		return nil
	}); err != nil {
		return fail(err)
	}
	return dc, nil
}
