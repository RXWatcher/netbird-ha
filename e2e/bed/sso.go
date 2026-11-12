package bed

import (
	"context"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// DexDeviceLogin completes the device authorization flow that a headless
// client starts (it prints http://lb/oauth2/device?user_code=XXXX-XXXX) by
// walking the embedded Dex pages: the user-code form, the connector choice,
// the password form, and any approval form. Requests to lb:80 go to
// lbHostPort, the load balancer's published port on the host.
func DexDeviceLogin(ctx context.Context, lbHostPort, deviceURL, email, password string) error {
	jar, _ := cookiejar.New(nil)
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	client := &http.Client{
		Jar:     jar,
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == "lb:80" {
					addr = lbHostPort
				}
				return dialer.DialContext(ctx, network, addr)
			},
		},
	}
	do := func(req *http.Request) (*url.URL, string, error) {
		resp, err := client.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		bs, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 400 {
			return nil, "", fmt.Errorf("HTTP %d at %s: %.300s", resp.StatusCode, resp.Request.URL, string(bs))
		}
		return resp.Request.URL, string(bs), nil
	}
	get := func(u string) (*url.URL, string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, "", err
		}
		return do(req)
	}
	post := func(base *url.URL, action string, form url.Values) (*url.URL, string, error) {
		target, err := base.Parse(action)
		if err != nil {
			return nil, "", err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), strings.NewReader(form.Encode()))
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return do(req)
	}

	at, page, err := get(deviceURL)
	if err != nil {
		return fmt.Errorf("open device page: %w", err)
	}
	userCode := ""
	if u, err := url.Parse(deviceURL); err == nil {
		userCode = u.Query().Get("user_code")
	}
	for step := 0; step < 8; step++ {
		lower := strings.ToLower(page)
		switch {
		case strings.Contains(lower, `name="user_code"`):
			at, page, err = post(at, formAction(page), url.Values{"user_code": {userCode}})
		case strings.Contains(lower, `name="password"`):
			at, page, err = post(at, formAction(page), url.Values{"login": {email}, "password": {password}})
		case strings.Contains(lower, `name="approval"`):
			form := url.Values{"approval": {"approve"}}
			if m := hiddenReqRe.FindStringSubmatch(page); m != nil {
				form.Set("req", html.UnescapeString(m[1]))
			}
			at, page, err = post(at, formAction(page), form)
		case connectorRe.MatchString(page):
			at, page, err = get(mustJoin(at, html.UnescapeString(connectorRe.FindStringSubmatch(page)[1])))
		case strings.Contains(lower, "device") && (strings.Contains(lower, "success") || strings.Contains(lower, "return to your device")):
			return nil
		default:
			return fmt.Errorf("unexpected page at %s: %.400s", at, page)
		}
		if err != nil {
			return err
		}
		if strings.Contains(strings.ToLower(page), "invalid") && strings.Contains(strings.ToLower(page), "nb-error") {
			return fmt.Errorf("Dex rejected the input at %s", at)
		}
	}
	return fmt.Errorf("device login did not finish (last page %s)", at)
}

var (
	formRe      = regexp.MustCompile(`(?is)<form[^>]*action="([^"]*)"`)
	connectorRe = regexp.MustCompile(`(?is)<a href="([^"]*/auth/local[^"]*)"`)
	hiddenReqRe = regexp.MustCompile(`(?is)name="req"\s+value="([^"]*)"`)
)

func formAction(page string) string {
	if m := formRe.FindStringSubmatch(page); m != nil {
		return html.UnescapeString(m[1])
	}
	return ""
}

func mustJoin(base *url.URL, ref string) string {
	u, err := base.Parse(ref)
	if err != nil {
		return ref
	}
	return u.String()
}
