// Package pineroute keeps destination-scoped route state for Pine egress.
//
// A SOCKS5 CONNECT refusal such as "not allowed by ruleset" or "connection
// refused" is about one destination, not about the proxy route. Marking the
// route failed for such a reply puts it into cooldown for every destination,
// and a page with many refused hosts can put every route of a Computer into
// cooldown at once. Instead, the refused route is skipped for the rest of the
// request and, for a short time, for later requests to the same host.
package pineroute

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-gost/core/chain"
)

const (
	replyNotAllowed  = 2
	replyConnRefused = 5

	refusalTTL        = 10 * time.Minute
	refusalCacheLimit = 4096
)

// ErrNoRoute reports that every configured route was excluded for a request.
var ErrNoRoute = errors.New("pine egress: no route available")

type socks5ReplyError interface {
	SOCKS5ReplyCode() uint8
}

// DestinationScoped reports whether err is a CONNECT refusal that concerns the
// destination rather than the health of the proxy route.
func DestinationScoped(err error) bool {
	var reply socks5ReplyError
	if !errors.As(err, &reply) {
		return false
	}
	switch reply.SOCKS5ReplyCode() {
	case replyNotAllowed, replyConnRefused:
		return true
	}
	return false
}

type attemptsKey struct{}

type attempts struct {
	mu    sync.Mutex
	tried map[*chain.Node]struct{}
}

// WithAttempts returns a context that tracks the routes tried by one request.
func WithAttempts(ctx context.Context) context.Context {
	if _, ok := ctx.Value(attemptsKey{}).(*attempts); ok {
		return ctx
	}
	return context.WithValue(ctx, attemptsKey{}, &attempts{tried: map[*chain.Node]struct{}{}})
}

// Tracking reports whether ctx belongs to a request that tracks attempts.
func Tracking(ctx context.Context) bool {
	_, ok := ctx.Value(attemptsKey{}).(*attempts)
	return ok
}

// MarkTried records that node was attempted by the request in ctx.
func MarkTried(ctx context.Context, node *chain.Node) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil || node == nil {
		return
	}
	a.mu.Lock()
	a.tried[node] = struct{}{}
	a.mu.Unlock()
}

func tried(ctx context.Context, node *chain.Node) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.tried[node]
	return ok
}

// Skip reports whether node should not be selected for host in ctx: it was
// already tried by this request, or it refused this host recently.
func Skip(ctx context.Context, node *chain.Node, host string) bool {
	if node == nil || !Tracking(ctx) {
		return false
	}
	return tried(ctx, node) || defaultRefusals.contains(node.Name, host, time.Now())
}

// RecordRefusal remembers that node refused host.
func RecordRefusal(node *chain.Node, host string) {
	if node == nil {
		return
	}
	defaultRefusals.add(node.Name, host, time.Now())
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

func refusalKey(nodeName, host string) (string, bool) {
	host = normalizeHost(host)
	if nodeName == "" || host == "" {
		return "", false
	}
	return nodeName + "\x00" + host, true
}

func normalizeHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func (c *refusalCache) add(nodeName, host string, now time.Time) {
	key, ok := refusalKey(nodeName, host)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.limit {
		for k, expires := range c.entries {
			if !now.Before(expires) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.limit {
			c.entries = map[string]time.Time{}
		}
	}
	c.entries[key] = now.Add(c.ttl)
}

func (c *refusalCache) contains(nodeName, host string, now time.Time) bool {
	key, ok := refusalKey(nodeName, host)
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
