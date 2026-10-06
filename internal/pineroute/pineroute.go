// Package pineroute keeps destination-scoped route state for Pine egress.
//
// Destination failures must not put a shared proxy or the explicit direct
// fallback into cooldown for unrelated sites. They exclude only that route
// for the current request and briefly for the same network/host/port.
//
// A managed route that keeps failing destinations which another route then
// reaches at the same address is itself broken, for example a residential
// exit that answers "host unreachable" for everything. Once that happens for
// enough distinct hosts in a short window, the route is ejected (see
// ejection.go): selection prefers the other routes while any of them is
// usable, and falls back to the ejected route rather than failing when none
// is.
package pineroute

import (
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
	"github.com/go-gost/core/selector"
)

const (
	replyNotAllowed = 2

	// A repeated refusal of the same route and destination doubles the TTL
	// up to the class's cap; a success for the pair resets it.
	refusalTTL        = 10 * time.Minute
	refusalTTLCap     = 6 * time.Hour
	transientTTL      = 30 * time.Second
	transientTTLCap   = 10 * time.Minute
	refusalCacheLimit = 4096
	escalationWindow  = 30 * time.Second
	escalationHosts   = 3
)

// refusalClass separates vendor policy refusals from transient destination
// failures; each doubles from its own base.
type refusalClass uint8

const (
	classTransient refusalClass = iota
	classPolicy
)

// ErrNoRoute reports that every configured route was excluded for a request.
var ErrNoRoute = errors.New("pine egress: no route available")

type socks5ReplyError interface {
	SOCKS5ReplyCode() uint8
}

// Enabled reports whether GOST runs under the Pine session coordinator, which
// always sets the quality event socket. Other GOST configurations keep
// upstream behaviour.
var Enabled = os.Getenv("PINE_GOST_EVENT_SOCKET") != ""

// UpstreamFailure retains the phase in which a route failed. A SOCKS reply
// received while reaching another proxy must not be cached against the final
// destination. Preserve error identity for callers and telemetry.
func UpstreamFailure(err error) error {
	if !Enabled || err == nil {
		return err
	}
	return upstreamFailure{err}
}

type upstreamFailure struct{ error }

func (e upstreamFailure) Unwrap() error { return e.error }

func isUpstreamFailure(err error) bool {
	var upstream upstreamFailure
	return errors.As(err, &upstream)
}

func tcpNetwork(network string) bool {
	return network == "tcp" || network == "tcp4" || network == "tcp6"
}

// DestinationScoped reports whether err from a TCP CONNECT is a refusal that
// concerns the destination rather than the health of the proxy route.
func DestinationScoped(network string, err error) bool {
	if !Enabled || !tcpNetwork(network) || isUpstreamFailure(err) {
		return false
	}
	var reply socks5ReplyError
	if !errors.As(err, &reply) {
		return false
	}
	switch reply.SOCKS5ReplyCode() {
	case 2, 3, 4, 5, 6, 8:
		// Policy, network/host reachability, refusal, TTL and address-family
		// errors concern this destination. General server failure (1), an
		// unsupported CONNECT command (7), unknown replies and transport or
		// authentication failures still count against the shared route.
		return true
	}
	return false
}

// IsDirect identifies Pine's explicitly configured direct connector. An empty
// chain is deliberately not direct: it must fail closed in the router.
func IsDirect(node *chain.Node) bool {
	if node == nil || node.Options().Metadata == nil {
		return false
	}
	kind, _ := node.Options().Metadata.Get("pine_route_kind").(string)
	return kind == "direct"
}

// IgnoreFailure distinguishes request/destination failures from route health.
// For direct connectors, final Connect is the destination dial itself; there
// is no upstream proxy whose shared health could be inferred from its error.
func IgnoreFailure(ctx context.Context, node *chain.Node, network string, err error) bool {
	return Enabled && err != nil && (RequestCanceled(ctx, err) ||
		!isUpstreamFailure(err) && (DestinationScoped(network, err) || IsDirect(node) && tcpNetwork(network)))
}

// RequestCanceled excludes caller cancellation, not the router's own per-route
// timeout. A slow proxy must still enter cooldown when its attempt expires.
func RequestCanceled(ctx context.Context, err error) bool {
	if !Enabled || err == nil {
		return false
	}
	request := ctx
	if a, _ := ctx.Value(attemptsKey{}).(*attempts); a != nil {
		request = a.request
	}
	return request.Err() != nil || errors.Is(err, context.Canceled)
}

// routeKey follows the connector's lifetime. Managed routes have a stable ID
// assigned by Pine. Direct nodes use the marker created by chain.NewNode:
// Node.Copy preserves it, while a separately loaded connector gets a new one.
// Retaining that identity in the bounded cache also prevents pointer reuse;
// neither names nor formatted pointer addresses are sufficient cache keys.
type routeKey struct {
	managedID string
	direct    selector.Marker
}

func nodeRouteKey(node *chain.Node) routeKey {
	if node == nil {
		return routeKey{}
	}
	if IsDirect(node) {
		return routeKey{direct: node.Marker()}
	}
	md := node.Options().Metadata
	if md == nil {
		return routeKey{}
	}
	id, _ := md.Get("pine_route_id").(string)
	return routeKey{managedID: id}
}

type attemptsKey struct{}

type attempts struct {
	mu       sync.Mutex
	tried    map[routeKey]struct{}
	suspects []suspect
	records  []attemptRecord
	// hops maps each managed route seen by this request's hop selection to
	// the hop's managed routes, for the ejection cap.
	hops map[incarnation][]incarnation
	// allRefused: see NoteAllRefused.
	allRefused bool
	request    context.Context
}

// WithAttempts returns a context that tracks the routes tried by one router
// dial. A nested dial, such as DNS resolution through the chain, gets its own
// set so its attempts do not exclude routes for the outer request.
func WithAttempts(ctx context.Context) context.Context {
	if !Enabled {
		return ctx
	}
	return context.WithValue(ctx, attemptsKey{}, &attempts{tried: map[routeKey]struct{}{}, request: ctx})
}

// Tracking reports whether ctx belongs to a request that tracks attempts.
func Tracking(ctx context.Context) bool {
	_, ok := ctx.Value(attemptsKey{}).(*attempts)
	return ok
}

// MarkTried records that node was attempted by the request in ctx.
func MarkTried(ctx context.Context, node *chain.Node) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	id := nodeRouteKey(node)
	if a == nil || id == (routeKey{}) {
		return
	}
	a.mu.Lock()
	a.tried[id] = struct{}{}
	a.mu.Unlock()
}

func tried(ctx context.Context, id routeKey) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.tried[id]
	return ok
}

// Skip reports whether node should not be selected: it was already tried by
// this request, or this connector refused the network/endpoint recently.
func Skip(ctx context.Context, node *chain.Node, network, address string) bool {
	id := nodeRouteKey(node)
	if id == (routeKey{}) || !Tracking(ctx) {
		return false
	}
	return tried(ctx, id) || defaultRefusals.contains(id, network, address, time.Now())
}

// RecordRefusal remembers a destination failure. Cancellation is not cached.
// Transient failures recover quickly; repeated cache hits never renew a TTL.
// A refusal of a pair whose entry expired but is still remembered doubles
// the TTL.
func RecordRefusal(ctx context.Context, node *chain.Node, network, address string, err error) {
	if !IgnoreFailure(ctx, node, network, err) || RequestCanceled(ctx, err) {
		return
	}
	class := classTransient
	var reply socks5ReplyError
	if errors.As(err, &reply) && reply.SOCKS5ReplyCode() == replyNotAllowed {
		class = classPolicy
	}
	if id := nodeRouteKey(node); id != (routeKey{}) {
		defaultRefusals.add(id, network, address, class, time.Now())
	}
}

// RecordSuccess forgets the refusal history of a pair the route just reached.
func RecordSuccess(ctx context.Context, node *chain.Node, network, address string) {
	if !Enabled || !Tracking(ctx) {
		return
	}
	if id := nodeRouteKey(node); id != (routeKey{}) {
		defaultRefusals.remove(id, network, address)
	}
}

type refusalCache struct {
	mu      sync.Mutex
	entries map[endpointKey]refusalEntry
	limit   int
}

// refusalEntry excludes its pair until expires. It is remembered for one more
// ttl after that, so a repeat refusal can be recognised and doubled.
type refusalEntry struct {
	expires time.Time
	ttl     time.Duration
	class   refusalClass
}

func (e refusalEntry) forgotten(now time.Time) bool { return !now.Before(e.expires.Add(e.ttl)) }

func classTTL(class refusalClass) (base, limit time.Duration) {
	if class == classPolicy {
		return refusalTTL, refusalTTLCap
	}
	return transientTTL, transientTTLCap
}

type endpointKey struct {
	route   routeKey
	network string
	address string
}

var defaultRefusals = newRefusalCache(refusalCacheLimit)

func newRefusalCache(limit int) *refusalCache {
	return &refusalCache{entries: map[endpointKey]refusalEntry{}, limit: limit}
}

func refusalKey(route routeKey, network, address string) (endpointKey, bool) {
	host, port, err := net.SplitHostPort(address)
	p, portErr := strconv.ParseUint(port, 10, 16)
	if route == (routeKey{}) || host == "" || err != nil || portErr != nil || p == 0 ||
		!tcpNetwork(network) {
		return endpointKey{}, false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.String()
	} else {
		host = strings.ToLower(strings.TrimSuffix(host, "."))
	}
	return endpointKey{route: route, network: network, address: net.JoinHostPort(host, strconv.FormatUint(p, 10))}, true
}

// add records a refusal of class for the pair. While the pair is excluded, a
// refusal (from a CONNECT that began before the entry existed) does not
// escalate: the same class keeps the later expiry at the current TTL, a
// policy refusal replaces a transient entry at the policy base, and a
// transient refusal never shortens a policy entry. After the exclusion ends,
// a refusal of the same class doubles the previous TTL up to the class's
// cap; otherwise the class's base applies.
func (c *refusalCache) add(route routeKey, network, address string, class refusalClass, now time.Time) {
	key, ok := refusalKey(route, network, address)
	if !ok || c.limit <= 0 {
		return
	}
	base, limit := classTTL(class)
	c.mu.Lock()
	defer c.mu.Unlock()
	ttl := base
	if previous, ok := c.entries[key]; ok && !previous.forgotten(now) {
		switch {
		case now.Before(previous.expires) && previous.class == class:
			if expires := now.Add(previous.ttl); expires.After(previous.expires) {
				previous.expires = expires
				c.entries[key] = previous
			}
			return
		case now.Before(previous.expires) && class == classTransient:
			return
		case now.Before(previous.expires):
			// A policy refusal while a transient entry is live starts the
			// policy class at its base.
		case previous.class == class:
			ttl = min(2*previous.ttl, limit)
		}
	} else if !ok && len(c.entries) >= c.limit {
		c.evictLocked(now)
	}
	c.entries[key] = refusalEntry{expires: now.Add(ttl), ttl: ttl, class: class}
}

// evictLocked drops forgotten entries, then the one forgotten soonest.
func (c *refusalCache) evictLocked(now time.Time) {
	var oldestKey endpointKey
	var oldest time.Time
	for k, entry := range c.entries {
		forget := entry.expires.Add(entry.ttl)
		if !now.Before(forget) {
			delete(c.entries, k)
			continue
		}
		if oldest.IsZero() || forget.Before(oldest) {
			oldestKey, oldest = k, forget
		}
	}
	if len(c.entries) >= c.limit {
		delete(c.entries, oldestKey)
	}
}

func (c *refusalCache) contains(route routeKey, network, address string, now time.Time) bool {
	key, ok := refusalKey(route, network, address)
	if !ok {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return false
	}
	if entry.forgotten(now) {
		delete(c.entries, key)
		return false
	}
	return now.Before(entry.expires)
}

func (c *refusalCache) remove(route routeKey, network, address string) {
	key, ok := refusalKey(route, network, address)
	if !ok {
		return
	}
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// incarnation identifies one loaded managed route. The marker is preserved by
// Node.Copy and differs for a route loaded again by a later plan, so evidence
// about a replaced connector does not carry over to its successor.
type incarnation struct {
	id     string
	marker selector.Marker
}

func managedIncarnation(node *chain.Node) (incarnation, bool) {
	if node == nil || IsDirect(node) {
		return incarnation{}, false
	}
	id := nodeRouteKey(node).managedID
	if id == "" || node.Marker() == nil {
		return incarnation{}, false
	}
	return incarnation{id: id, marker: node.Marker()}, true
}

// suspect is a managed route that failed this request's destination with a
// transient reply. It is charged only if another route then reaches the same
// dialed address within the same request.
type suspect struct {
	route  incarnation
	info   routeInfo
	host   string
	dialed string
	at     time.Time
}

func transientDestinationReply(network string, err error) bool {
	if !DestinationScoped(network, err) {
		return false
	}
	var reply socks5ReplyError
	errors.As(err, &reply)
	switch reply.SOCKS5ReplyCode() {
	case 3, 4, 5, 6:
		// Network/host unreachable, connection refused and TTL expiry. Policy
		// refusals (2) concern the destination, and address-family errors (8)
		// concern only one family, so neither is evidence against the route.
		return true
	}
	return false
}

// NoteSuspect records a transient destination failure of node while dialing
// dialed (the address actually sent in CONNECT) for address.
func NoteSuspect(ctx context.Context, node *chain.Node, network, address, dialed string, err error) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	route, ok := managedIncarnation(node)
	host := destinationHost(address)
	if a == nil || !ok || host == "" || dialed == "" || !transientDestinationReply(network, err) ||
		RequestCanceled(ctx, err) {
		return
	}
	a.mu.Lock()
	a.suspects = append(a.suspects, suspect{route: route, info: nodeRouteInfo(node), host: host, dialed: network + "/" + dialed, at: time.Now()})
	a.mu.Unlock()
}

// BlameSuspects is called after winner connected network/dialed for the
// request in ctx. Earlier suspects that failed the same dialed address on
// another route are charged; a route charged with escalationHosts distinct
// hosts within escalationWindow is ejected, subject to the ejection cap.
// Destinations that no route reaches are never charged.
func BlameSuspects(ctx context.Context, winner *chain.Node, network, dialed string) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil || winner == nil {
		return
	}
	a.mu.Lock()
	suspects := a.suspects
	a.suspects = nil
	a.mu.Unlock()
	if len(suspects) == 0 || a.request.Err() != nil {
		return
	}
	winnerRoute, _ := managedIncarnation(winner)
	target := network + "/" + dialed
	now := time.Now()
	for _, s := range suspects {
		if s.dialed != target || s.route == winnerRoute {
			continue
		}
		if defaultEscalations.charge(s.route, s.host, s.at, now) {
			defaultEjections.eject(s.route, s.info, hopOf(ctx, s.route), ReasonDestinationFailures)
		} else {
			defaultEjections.noteBad(s.route)
		}
	}
}

func destinationHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return ""
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.String()
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

type escalationTracker struct {
	mu     sync.Mutex
	routes map[incarnation][]hostSeen
	limit  int
}

type hostSeen struct {
	host string
	at   time.Time
}

var defaultEscalations = newEscalationTracker(refusalCacheLimit)

func newEscalationTracker(limit int) *escalationTracker {
	return &escalationTracker{routes: map[incarnation][]hostSeen{}, limit: limit}
}

// charge records that route failed host at failedAt and reports whether the
// route has failed escalationHosts distinct hosts within escalationWindow of
// now. Recency is measured from the failure, not from when it was charged, so
// a delayed attribution cannot refresh old evidence. The route's history is
// cleared when it escalates.
func (t *escalationTracker) charge(route incarnation, host string, failedAt, now time.Time) bool {
	if route.id == "" || host == "" || t.limit <= 0 || now.Sub(failedAt) >= escalationWindow {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	seen, known := t.routes[route]
	if !known && len(t.routes) >= t.limit {
		t.evict(now)
	}
	recent := make([]hostSeen, 0, len(seen)+1)
	for _, h := range seen {
		if now.Sub(h.at) >= escalationWindow {
			continue
		}
		if h.host == host {
			if h.at.After(failedAt) {
				failedAt = h.at
			}
			continue
		}
		recent = append(recent, h)
	}
	recent = append(recent, hostSeen{host: host, at: failedAt})
	if len(recent) >= escalationHosts {
		delete(t.routes, route)
		return true
	}
	t.routes[route] = recent
	return false
}

func (t *escalationTracker) evict(now time.Time) {
	var oldestKey incarnation
	var oldest time.Time
	for k, seen := range t.routes {
		latest := seen[0].at
		for _, h := range seen[1:] {
			if h.at.After(latest) {
				latest = h.at
			}
		}
		if now.Sub(latest) >= escalationWindow {
			delete(t.routes, k)
			continue
		}
		if oldest.IsZero() || latest.Before(oldest) {
			oldestKey, oldest = k, latest
		}
	}
	if len(t.routes) >= t.limit {
		delete(t.routes, oldestKey)
	}
}
