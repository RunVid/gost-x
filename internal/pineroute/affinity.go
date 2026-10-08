package pineroute

// Per-site route affinity: sites (registered domains) start in fifo order. A
// site that ends up on another route than its default (failover, refusal) is
// pinned there so it does not flip back, but only to a route in Chrome's
// timezone. Login sites are pinned from their first success and held through
// transient failures for a bounded time. See hop.selectPinned for the rules.

import (
	"container/list"
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-gost/core/chain"
	"golang.org/x/net/publicsuffix"
)

const (
	defaultAffinitySites     = 4096
	defaultAffinityTTL       = 24 * time.Hour
	defaultAffinityLoginHold = 2 * time.Minute
	// loginHoldFailures is the minimum number of consecutive failures of a
	// held login pin before the hold may expire.
	loginHoldFailures = 3
)

// Reasons carried by affinity_moved events.
const (
	MoveSiteRefused  = "site_refused"
	MoveRouteEjected = "route_ejected"
	MoveRouteRemoved = "route_removed"
	// MoveRouteFailed moves a login site whose hold expired while its route
	// kept failing route-wide. Other sites only detour on route failures.
	MoveRouteFailed = "route_failed"
)

// Affinity is the per-hop affinity configuration, rendered by the session
// coordinator into the hop's metadata so it changes atomically with the
// routes on a config reload.
type Affinity struct {
	// timeZone is Chrome's timezone: pins only target routes whose
	// pine_route_tz equals it.
	timeZone string
	exact    map[string]struct{}
	// suffixes holds ".domain" matchers without the dot; they match the
	// domain and every subdomain.
	suffixes map[string]struct{}
}

// NewAffinity returns the affinity configuration for a hop, or nil when GOST
// does not run under Pine or Chrome's timezone is unknown: without it no pin
// could be checked against Chrome's identity, so the hop stays plain fifo.
// loginSites uses the address-matcher forms "host" and ".domain".
func NewAffinity(timeZone string, loginSites []string) *Affinity {
	if !Enabled || timeZone == "" {
		return nil
	}
	a := &Affinity{timeZone: timeZone, exact: map[string]struct{}{}, suffixes: map[string]struct{}{}}
	for _, matcher := range loginSites {
		if domain, ok := strings.CutPrefix(matcher, "."); ok {
			if domain != "" {
				a.suffixes[domain] = struct{}{}
			}
		} else if matcher != "" {
			a.exact[matcher] = struct{}{}
		}
	}
	return a
}

// LoginSite reports whether the CONNECT host or host:port is a login site,
// compared as sent like GOST's address matcher for the ISP whitelist.
func (a *Affinity) LoginSite(address string) bool {
	if a == nil {
		return false
	}
	host := hostOf(address)
	if host == "" {
		return false
	}
	if _, ok := a.exact[host]; ok {
		return true
	}
	for candidate := host; ; {
		if _, ok := a.suffixes[candidate]; ok {
			return true
		}
		index := strings.IndexByte(candidate, '.')
		if index < 0 {
			return false
		}
		candidate = candidate[index+1:]
	}
}

func hostOf(address string) string {
	if h, _, err := net.SplitHostPort(address); err == nil {
		return h
	}
	return address
}

func normalizeHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
}

var defaultPins = newPinTable(
	envInt("PINE_GOST_AFFINITY_MAX_SITES", defaultAffinitySites),
	envDuration("PINE_GOST_AFFINITY_TTL", defaultAffinityTTL),
)

var loginHold = envDuration("PINE_GOST_AFFINITY_LOGIN_HOLD", defaultAffinityLoginHold)

func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}

// SiteKey returns the affinity key of a CONNECT host or host:port: the
// registered domain (eTLD+1), the canonical address for an IP literal, or the
// host itself when it has no registered domain (localhost, a bare suffix).
func SiteKey(address string) string {
	host := normalizeHost(hostOf(address))
	if host == "" {
		return ""
	}
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		return ip.Unmap().String()
	}
	if site, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		return site
	}
	return host
}

func managedRouteID(node *chain.Node) string {
	if node == nil || IsDirect(node) {
		return ""
	}
	return nodeRouteKey(node).managedID
}

type pinKey struct {
	site  string
	login bool
}

// SiteSelection is the affinity state of one router dial: its key, the pin
// it started from, and why it may move away from that pin. A dial's
// attempts run one after another, so it needs no lock.
type SiteSelection struct {
	key      pinKey
	timeZone string
	pinned   string
	// generation identifies the pin the request started from; updates from a
	// request that saw an older pin are ignored.
	generation uint64
	// defaultRoute is the first route the hop offers for the host, before
	// transient state filters any: where fifo puts the site. It needs no pin.
	defaultRoute string
	// reason is set once the request has a reason to leave its pin; stop when
	// a held login site must fail rather than try another route.
	reason string
	stop   bool
	held   bool
}

// Site returns the affinity state of the request in ctx for host (the CONNECT
// host:port), creating it on the first selection with the hop's affinity
// configuration. Nil when the hop has no affinity or the request is not
// tracked.
func Site(ctx context.Context, network, host string, affinity *Affinity) *SiteSelection {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if affinity == nil || a == nil || !tcpNetwork(network) {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.site == nil {
		site := SiteKey(host)
		if site == "" {
			return nil
		}
		key := pinKey{site: site, login: affinity.LoginSite(host)}
		route, generation := defaultPins.get(key, time.Now())
		a.site = &SiteSelection{key: key, timeZone: affinity.timeZone, pinned: route, generation: generation}
	}
	return a.site
}

func siteFromContext(ctx context.Context) *SiteSelection {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.site
}

// IsPinned reports whether node carries the route this request is pinned to.
func (s *SiteSelection) IsPinned(node *chain.Node) bool {
	return s != nil && s.pinned != "" && managedRouteID(node) == s.pinned
}

// Pinned reports whether the request started with a pin.
func (s *SiteSelection) Pinned() bool { return s != nil && s.pinned != "" }

// Stopped reports whether a held login site must fail instead of trying
// another route after its pinned route failed.
func (s *SiteSelection) Stopped() bool { return s.stop }

// Move records why the request may leave its pin. The first reason wins,
// except that a route-wide ejection is always reported as such.
func (s *SiteSelection) Move(reason string) {
	if s != nil && s.pinned != "" && (s.reason == "" || reason == MoveRouteEjected) {
		s.reason = reason
	}
}

// Hold reports whether a login site must stay on its pinned route through a
// transient refusal or a route failure. A pin that has kept failing for the
// hold window is released so the site stays reachable.
func (s *SiteSelection) Hold() bool {
	if s == nil || !s.key.login || s.pinned == "" {
		return false
	}
	if !s.held {
		failures, since := defaultPins.streak(s.key, s.generation)
		if failures >= loginHoldFailures && time.Since(since) >= loginHold {
			return false
		}
		s.held = true
	}
	return true
}

// HoldRouteFailure reports whether a login site holds its pinned route
// through a route failure. Once its hold expired it moves (route_failed);
// other sites only detour.
func (s *SiteSelection) HoldRouteFailure() bool {
	if s.Hold() {
		return true
	}
	if s.key.login {
		s.Move(MoveRouteFailed)
	}
	return false
}

// SetDefault records node as the site's default route; the first call of
// the request wins.
func (s *SiteSelection) SetDefault(node *chain.Node) {
	if s != nil && s.defaultRoute == "" {
		s.defaultRoute = managedRouteID(node)
	}
}

// PinAllowed reports whether node may hold a pin for this site: a managed
// route whose exit is in Chrome's timezone. Other routes only ever serve as
// detours.
func (s *SiteSelection) PinAllowed(node *chain.Node) bool {
	if s == nil || managedRouteID(node) == "" || node.Options().Metadata == nil {
		return false
	}
	zone, _ := node.Options().Metadata.Get("pine_route_tz").(string)
	return zone != "" && zone == s.timeZone
}

// wantsPin reports whether a site that connected on winner should be pinned
// there: any allowed route for a login site (the hold needs it), any allowed
// route but its default for other sites.
func (s *SiteSelection) wantsPin(winner *chain.Node, route string) bool {
	return route != "" && s.PinAllowed(winner) && (s.key.login || route != s.defaultRoute)
}

// RefusedFor reports whether the refusal cache holds node for address, and
// whether that entry is a policy refusal rather than a transient one.
func RefusedFor(node *chain.Node, network, address string) (refused, policy bool) {
	id := nodeRouteKey(node)
	if id == (routeKey{}) {
		return false, false
	}
	return defaultRefusals.lookup(id, network, address, time.Now())
}

// Tried reports whether node was already attempted by the request in ctx.
func Tried(ctx context.Context, node *chain.Node) bool {
	id := nodeRouteKey(node)
	return id != (routeKey{}) && tried(ctx, id)
}

// AffinityAttempt records a failure of the pinned route: it extends the
// pin's failure streak and decides what the request does next. A held login
// site stops (except on a policy refusal); a refusal of the site moves the
// pin; a route failure detours. Caller cancellation is ignored.
func AffinityAttempt(ctx context.Context, node *chain.Node, network string, err error) {
	s := siteFromContext(ctx)
	if s == nil || err == nil || !s.IsPinned(node) || RequestCanceled(ctx, err) {
		return
	}
	defaultPins.fail(s.key, s.generation, time.Now())
	refused := DestinationScoped(network, err)
	var reply socks5ReplyError
	policy := refused && errors.As(err, &reply) && reply.SOCKS5ReplyCode() == replyNotAllowed
	switch {
	case refused && !policy && s.Hold():
		s.stop = true
	case refused:
		s.Move(MoveSiteRefused)
	default:
		s.stop = s.HoldRouteFailure()
	}
}

// AffinityMove describes a re-pin, for the affinity_moved event.
type AffinityMove struct {
	Site      string
	FromRoute string
	ToRoute   string
	Reason    string
	LoginSite bool
}

// AffinitySuccess updates the pin after winner connected the request's site.
// An unpinned site is pinned only when it connected on an allowed route
// other than its default (login sites: any allowed route). A pinned site
// leaves its pin only when the request had a reason to, and only while the
// pin is still the one the request started from: the pin moves to winner if
// winner should hold one, else it is dropped (the site is back on fifo). The
// move is returned for managed winners; a direct winner drops the pin
// silently. A detour (no reason) never changes the pin.
func AffinitySuccess(ctx context.Context, winner *chain.Node) *AffinityMove {
	s := siteFromContext(ctx)
	if s == nil {
		return nil
	}
	route, reason, now := managedRouteID(winner), s.reason, time.Now()
	switch {
	case s.pinned == "":
		if s.wantsPin(winner, route) {
			defaultPins.swap(s.key, 0, route, now)
		}
		return nil
	case route == s.pinned && reason == "":
		defaultPins.swap(s.key, s.generation, route, now)
		return nil
	case route == s.pinned:
		// A pin marked for removal (ejected, outside Chrome's timezone) that
		// still served as the last usable route is dropped, not refreshed.
		defaultPins.remove(s.key, s.generation, now)
		return nil
	case reason == "":
		return nil
	}
	if s.wantsPin(winner, route) {
		if !defaultPins.swap(s.key, s.generation, route, now) {
			return nil
		}
	} else if !defaultPins.remove(s.key, s.generation, now) || route == "" {
		return nil
	}
	return &AffinityMove{Site: s.key.site, FromRoute: s.pinned, ToRoute: route, Reason: reason, LoginSite: s.key.login}
}

// pinTable is a bounded LRU of site pins with a sliding TTL: a pin expires
// when its route has not served the site for ttl.
type pinTable struct {
	mu      sync.Mutex
	entries map[pinKey]*list.Element
	lru     *list.List
	limit   int
	ttl     time.Duration
	// next is the last generation handed out; every pin (re)assignment gets
	// a new one, so a stale request cannot act on a newer pin even if it
	// names the same route.
	next uint64
}

type pinEntry struct {
	key        pinKey
	route      string
	generation uint64
	lastUsed   time.Time
	// failures counts consecutive failures of route for this key since
	// failingSince; a success clears them.
	failures     int
	failingSince time.Time
}

func newPinTable(limit int, ttl time.Duration) *pinTable {
	return &pinTable{entries: map[pinKey]*list.Element{}, lru: list.New(), limit: limit, ttl: ttl}
}

// entry returns the live entry for key, dropping it if it expired.
func (t *pinTable) entry(key pinKey, now time.Time) *pinEntry {
	element, ok := t.entries[key]
	if !ok {
		return nil
	}
	entry := element.Value.(*pinEntry)
	if now.Sub(entry.lastUsed) >= t.ttl {
		t.lru.Remove(element)
		delete(t.entries, key)
		return nil
	}
	return entry
}

// get returns key's route and pin generation; generation 0 means unpinned.
func (t *pinTable) get(key pinKey, now time.Time) (string, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entry(key, now)
	if entry == nil {
		return "", 0
	}
	return entry.route, entry.generation
}

// swap sets key to route if its current pin generation is expect (0 =
// unpinned), marks it used and clears its failure streak. Keeping the same
// route keeps the generation. It reports whether it changed the table.
func (t *pinTable) swap(key pinKey, expect uint64, route string, now time.Time) bool {
	if t.limit <= 0 || route == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entry(key, now)
	var current uint64
	if entry != nil {
		current = entry.generation
	}
	if current != expect {
		return false
	}
	if entry == nil {
		for t.lru.Len() >= t.limit {
			oldest := t.lru.Back()
			t.lru.Remove(oldest)
			delete(t.entries, oldest.Value.(*pinEntry).key)
		}
		entry = &pinEntry{key: key}
		t.entries[key] = t.lru.PushFront(entry)
	} else {
		t.lru.MoveToFront(t.entries[key])
	}
	if entry.route != route || entry.generation == 0 {
		t.next++
		entry.generation = t.next
	}
	entry.route, entry.lastUsed = route, now
	entry.failures, entry.failingSince = 0, time.Time{}
	return true
}

// remove drops key's pin if it is still generation, and reports whether it
// did.
func (t *pinTable) remove(key pinKey, generation uint64, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entry(key, now)
	if entry == nil || entry.generation != generation {
		return false
	}
	t.lru.Remove(t.entries[key])
	delete(t.entries, key)
	return true
}

// fail extends the failure streak of key while its pin is still generation.
// It does not refresh the pin's TTL.
func (t *pinTable) fail(key pinKey, generation uint64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := t.entry(key, now)
	if entry == nil || entry.generation != generation {
		return
	}
	if entry.failures == 0 {
		entry.failingSince = now
	}
	entry.failures++
}

func (t *pinTable) streak(key pinKey, generation uint64) (int, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	element, ok := t.entries[key]
	if !ok {
		return 0, time.Time{}
	}
	entry := element.Value.(*pinEntry)
	if entry.generation != generation {
		return 0, time.Time{}
	}
	return entry.failures, entry.failingSince
}

// SetLoginHold replaces the login hold window and returns the previous one.
// For tests.
func SetLoginHold(d time.Duration) time.Duration {
	previous := loginHold
	loginHold = d
	return previous
}
