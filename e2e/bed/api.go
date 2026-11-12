package bed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// API is a minimal client for the management REST API, authenticated with a PAT.
type API struct {
	Base string
	PAT  string
	HTTP *http.Client
}

// NewAPI returns a client for the API at base (e.g. http://127.0.0.1:32786).
// Keep-alives are off so that every call opens a new connection and an L4
// load balancer places it afresh.
func NewAPI(base, pat string) *API {
	return &API{
		Base: base,
		PAT:  pat,
		HTTP: &http.Client{
			Timeout:   20 * time.Second,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
	}
}

// HTTPError is a non-2xx API response.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// Do sends a request to /api<path>; in (if not nil) is sent as JSON and the
// response is decoded into out (if not nil).
func (a *API) Do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		bs, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(bs)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.Base+"/api"+path, body)
	if err != nil {
		return err
	}
	if a.PAT != "" {
		req.Header.Set("Authorization", "Token "+a.PAT)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return &HTTPError{Status: resp.StatusCode, Body: string(bs)}
	}
	if out != nil && len(bs) > 0 {
		return json.Unmarshal(bs, out)
	}
	return nil
}

// GetRaw sends an unauthenticated GET to a path outside /api (e.g. /oauth2/keys)
// and fails unless it answers 2xx.
func (a *API) GetRaw(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.Base+path, nil)
	if err != nil {
		return err
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return &HTTPError{Status: resp.StatusCode, Body: string(bs)}
	}
	return nil
}

// Peer is the subset of the API's peer object the scenarios use.
type Peer struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Hostname  string  `json:"hostname"`
	DNSLabel  string  `json:"dns_label"`
	IP        string  `json:"ip"`
	Connected bool    `json:"connected"`
	Ephemeral bool    `json:"ephemeral"`
	UserID    string  `json:"user_id"`
	Groups    []Named `json:"groups"`
}

// Named is an {id, name} reference.
type Named struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Peers lists all peers.
func (a *API) Peers(ctx context.Context) ([]Peer, error) {
	var ps []Peer
	err := a.Do(ctx, http.MethodGet, "/peers", nil, &ps)
	return ps, err
}

// PeerByName returns the peer with the given name.
func (a *API) PeerByName(ctx context.Context, name string) (Peer, error) {
	ps, err := a.Peers(ctx)
	if err != nil {
		return Peer{}, err
	}
	for _, p := range ps {
		if p.Name == name || p.DNSLabel == name {
			return p, nil
		}
	}
	return Peer{}, fmt.Errorf("peer %q not found", name)
}

// DeletePeer deletes a peer.
func (a *API) DeletePeer(ctx context.Context, id string) error {
	return a.Do(ctx, http.MethodDelete, "/peers/"+id, nil, nil)
}

// Group is an API group.
type Group struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Peers []Named `json:"peers"`
}

// CreateGroup creates a group with the given peer IDs.
func (a *API) CreateGroup(ctx context.Context, name string, peerIDs []string) (Group, error) {
	var g Group
	err := a.Do(ctx, http.MethodPost, "/groups", map[string]any{"name": name, "peers": peerIDs}, &g)
	return g, err
}

// UpdateGroup replaces a group's peers.
func (a *API) UpdateGroup(ctx context.Context, id, name string, peerIDs []string) error {
	return a.Do(ctx, http.MethodPut, "/groups/"+id, map[string]any{"name": name, "peers": peerIDs}, nil)
}

// DeleteGroup deletes a group.
func (a *API) DeleteGroup(ctx context.Context, id string) error {
	return a.Do(ctx, http.MethodDelete, "/groups/"+id, nil, nil)
}

// Groups lists groups.
func (a *API) Groups(ctx context.Context) ([]Group, error) {
	var gs []Group
	err := a.Do(ctx, http.MethodGet, "/groups", nil, &gs)
	return gs, err
}

// Policy is an API policy as read back.
type Policy struct {
	ID      string       `json:"id"`
	Name    string       `json:"name"`
	Enabled bool         `json:"enabled"`
	Rules   []PolicyRule `json:"rules"`
}

// PolicyRule is one rule of a policy as read back.
type PolicyRule struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Enabled       bool    `json:"enabled"`
	Action        string  `json:"action"`
	Bidirectional bool    `json:"bidirectional"`
	Protocol      string  `json:"protocol"`
	Sources       []Named `json:"sources"`
	Destinations  []Named `json:"destinations"`
}

// Policies lists policies.
func (a *API) Policies(ctx context.Context) ([]Policy, error) {
	var ps []Policy
	err := a.Do(ctx, http.MethodGet, "/policies", nil, &ps)
	return ps, err
}

func policyBody(name string, enabled bool, src, dst []string) map[string]any {
	return map[string]any{
		"name":    name,
		"enabled": enabled,
		"rules": []map[string]any{{
			"name":          name,
			"enabled":       true,
			"action":        "accept",
			"bidirectional": true,
			"protocol":      "all",
			"sources":       src,
			"destinations":  dst,
		}},
	}
}

// CreatePolicy creates an accept-all, bidirectional policy between groups.
func (a *API) CreatePolicy(ctx context.Context, name string, enabled bool, srcGroups, dstGroups []string) (Policy, error) {
	var p Policy
	err := a.Do(ctx, http.MethodPost, "/policies", policyBody(name, enabled, srcGroups, dstGroups), &p)
	return p, err
}

// SetPolicyEnabled rewrites a policy read back from the API with enabled set.
func (a *API) SetPolicyEnabled(ctx context.Context, p Policy, enabled bool) error {
	rules := make([]map[string]any, 0, len(p.Rules))
	for _, r := range p.Rules {
		rules = append(rules, map[string]any{
			"name":          r.Name,
			"enabled":       r.Enabled,
			"action":        r.Action,
			"bidirectional": r.Bidirectional,
			"protocol":      r.Protocol,
			"sources":       ids(r.Sources),
			"destinations":  ids(r.Destinations),
		})
	}
	return a.Do(ctx, http.MethodPut, "/policies/"+p.ID, map[string]any{"name": p.Name, "enabled": enabled, "rules": rules}, nil)
}

// DeletePolicy deletes a policy.
func (a *API) DeletePolicy(ctx context.Context, id string) error {
	return a.Do(ctx, http.MethodDelete, "/policies/"+id, nil, nil)
}

func ids(ns []Named) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.ID)
	}
	return out
}

// SetupKey is a created setup key.
type SetupKey struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// CreateSetupKey creates a reusable setup key.
func (a *API) CreateSetupKey(ctx context.Context, name string, ephemeral bool, autoGroups []string) (SetupKey, error) {
	if autoGroups == nil {
		autoGroups = []string{}
	}
	var k SetupKey
	err := a.Do(ctx, http.MethodPost, "/setup-keys", map[string]any{
		"name": name, "type": "reusable", "expires_in": 86400, "auto_groups": autoGroups,
		"usage_limit": 0, "ephemeral": ephemeral,
	}, &k)
	return k, err
}

// User is the subset of the API's user object the scenarios use.
type User struct {
	ID         string   `json:"id"`
	Email      string   `json:"email"`
	Name       string   `json:"name"`
	Role       string   `json:"role"`
	IsBlocked  bool     `json:"is_blocked"`
	AutoGroups []string `json:"auto_groups"`
	Password   string   `json:"password"`
}

// CreateUser creates a local (embedded IdP) user; the response carries a generated password.
func (a *API) CreateUser(ctx context.Context, email, name string) (User, error) {
	var u User
	err := a.Do(ctx, http.MethodPost, "/users", map[string]any{
		"email": email, "name": name, "role": "user", "auto_groups": []string{}, "is_service_user": false,
	}, &u)
	return u, err
}

// SetUserBlocked blocks or unblocks a user.
func (a *API) SetUserBlocked(ctx context.Context, u User, blocked bool) error {
	ag := u.AutoGroups
	if ag == nil {
		ag = []string{}
	}
	return a.Do(ctx, http.MethodPut, "/users/"+u.ID, map[string]any{
		"role": u.Role, "auto_groups": ag, "is_blocked": blocked,
	}, nil)
}

// DeleteUser deletes a user.
func (a *API) DeleteUser(ctx context.Context, id string) error {
	return a.Do(ctx, http.MethodDelete, "/users/"+id, nil, nil)
}

// AccessiblePeers lists the peers a peer may reach according to the API.
func (a *API) AccessiblePeers(ctx context.Context, peerID string) ([]Peer, error) {
	var ps []Peer
	err := a.Do(ctx, http.MethodGet, "/peers/"+peerID+"/accessible-peers", nil, &ps)
	return ps, err
}
