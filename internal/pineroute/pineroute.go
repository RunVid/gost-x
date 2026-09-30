// Package pineroute keeps destination-scoped route state for Pine egress.
//
// A SOCKS5 CONNECT reply "not allowed by ruleset" is about one destination,
// not about the proxy route. Marking the route failed for such a reply puts it
// into cooldown for every destination, and a page with many refused hosts can
// put every route of a Computer into cooldown at once. Instead, the refused
// route is skipped for the rest of the request and, for a short time, for
// later requests to the same host.
package pineroute

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-gost/core/chain"
)

const (
	replyNotAllowed = 2

	refusalTTL        = 10 * time.Minute
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

// DestinationScoped reports whether err from a TCP CONNECT is a refusal that
// concerns the destination rather than the health of the proxy route.
func DestinationScoped(network string, err error) bool {
	if !Enabled || !strings.HasPrefix(network, "tcp") {
		return false
	}
	var reply socks5ReplyError
	return errors.As(err, &reply) && reply.SOCKS5ReplyCode() == replyNotAllowed
}

func routeID(node *chain.Node) string {
	if node == nil {
		return ""
	}
	md := node.Options().Metadata
	if md == nil {
		return ""
	}
	id, _ := md.Get("pine_route_id").(string)
	return id
}

type attemptsKey struct{}

type attempts struct {
	mu    sync.Mutex
	tried map[string]struct{}
}

// WithAttempts returns a context that tracks the routes tried by one router
// dial. A nested dial, such as DNS resolution through the chain, gets its own
// set so its attempts do not exclude routes for the outer request.
func WithAttempts(ctx context.Context) context.Context {
	if !Enabled {
		return ctx
	}
	return context.WithValue(ctx, attemptsKey{}, &attempts{tried: map[string]struct{}{}})
}

// Tracking reports whether ctx belongs to a request that tracks attempts.
func Tracking(ctx context.Context) bool {
	_, ok := ctx.Value(attemptsKey{}).(*attempts)
	return ok
}

// MarkTried records that node was attempted by the request in ctx.
func MarkTried(ctx context.Context, node *chain.Node) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	id := routeID(node)
	if a == nil || id == "" {
		return
	}
	a.mu.Lock()
	a.tried[id] = struct{}{}
	a.mu.Unlock()
}

func tried(ctx context.Context, id string) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.tried[id]
	return ok
}

// Skip reports whether node should not be selected for host in ctx: it was
// already tried by this request, or it refused this host recently.
func Skip(ctx context.Context, node *chain.Node, host string) bool {
	id := routeID(node)
	if id == "" || !Tracking(ctx) {
		return false
	}
	return tried(ctx, id) || defaultRefusals.contains(id, host, time.Now())
}

// RecordRefusal remembers that node refused host.
func RecordRefusal(node *chain.Node, host string) {
	if id := routeID(node); id != "" {
		defaultRefusals.add(id, host, time.Now())
	}
}

type refusalCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	ttl     time.Duration
	limit   int
}

var defaultRefusals = newRefusalCache(refusalTTL, refusalCacheLimit)

func newRefusalCache(ttl time.Duration, limit int) *refusalCache {
	return &refusalCache{entries: map[string]time.Time{}, ttl: ttl, limit: limit}
}

func refusalKey(routeID, host string) (string, bool) {
	host = normalizeHost(host)
	if routeID == "" || host == "" {
		return "", false
	}
	return routeID + "\x00" + host, true
}

func normalizeHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func (c *refusalCache) add(routeID, host string, now time.Time) {
	key, ok := refusalKey(routeID, host)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; !ok && len(c.entries) >= c.limit {
		oldestKey, oldest := "", time.Time{}
		for k, expires := range c.entries {
			if !now.Before(expires) {
				delete(c.entries, k)
				continue
			}
			if oldestKey == "" || expires.Before(oldest) {
				oldestKey, oldest = k, expires
			}
		}
		if len(c.entries) >= c.limit {
			delete(c.entries, oldestKey)
		}
	}
	c.entries[key] = now.Add(c.ttl)
}

func (c *refusalCache) contains(routeID, host string, now time.Time) bool {
	key, ok := refusalKey(routeID, host)
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
