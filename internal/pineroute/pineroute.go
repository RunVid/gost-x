// Package pineroute keeps destination-scoped route state for Pine egress.
//
// Destination failures must not put a shared proxy or the explicit direct
// fallback into cooldown for unrelated sites. They exclude only that route
// for the current request and briefly for the same network/host/port.
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

	refusalTTL        = 10 * time.Minute
	transientTTL      = 30 * time.Second
	refusalCacheLimit = 4096
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
	mu      sync.Mutex
	tried   map[routeKey]struct{}
	request context.Context
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
func RecordRefusal(ctx context.Context, node *chain.Node, network, address string, err error) {
	if !IgnoreFailure(ctx, node, network, err) || RequestCanceled(ctx, err) {
		return
	}
	ttl := transientTTL
	var reply socks5ReplyError
	if errors.As(err, &reply) && reply.SOCKS5ReplyCode() == replyNotAllowed {
		ttl = refusalTTL
	}
	if id := nodeRouteKey(node); id != (routeKey{}) {
		defaultRefusals.add(id, network, address, ttl, time.Now())
	}
}

type refusalCache struct {
	mu      sync.Mutex
	entries map[endpointKey]time.Time
	limit   int
}

type endpointKey struct {
	route   routeKey
	network string
	address string
}

var defaultRefusals = newRefusalCache(refusalCacheLimit)

func newRefusalCache(limit int) *refusalCache {
	return &refusalCache{entries: map[endpointKey]time.Time{}, limit: limit}
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

func (c *refusalCache) add(route routeKey, network, address string, ttl time.Duration, now time.Time) {
	key, ok := refusalKey(route, network, address)
	if !ok || c.limit <= 0 || ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; !ok && len(c.entries) >= c.limit {
		var oldestKey endpointKey
		var oldest time.Time
		for k, expires := range c.entries {
			if !now.Before(expires) {
				delete(c.entries, k)
				continue
			}
			if oldest.IsZero() || expires.Before(oldest) {
				oldestKey, oldest = k, expires
			}
		}
		if len(c.entries) >= c.limit {
			delete(c.entries, oldestKey)
		}
	}
	c.entries[key] = now.Add(ttl)
}

func (c *refusalCache) contains(route routeKey, network, address string, now time.Time) bool {
	key, ok := refusalKey(route, network, address)
	if !ok {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	expires, ok := c.entries[key]
	if !ok {
		return false
	}
	if !now.Before(expires) {
		delete(c.entries, key)
		return false
	}
	return true
}
