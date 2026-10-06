package pineroute

import (
	"context"
	"sync"
	"time"

	"github.com/go-gost/core/chain"
)

// When every route of a destination is excluded by its per-destination
// refusals, each browser retry fails at once with no route. Those failures
// are real but say nothing new, and a retrying page produces hundreds a
// minute. The first one in a window is reported as usual; the rest are
// counted and reported once, as one event with the count, when the window
// closes. The browser still sees every failure immediately.
const (
	noRouteMergeWindow = 10 * time.Second
	noRouteMergeLimit  = 1024
)

// maxSuppressedPerSummary keeps a summary inside the coordinator's accepted
// range (1,000,000); a window that reaches it reports a summary at once and
// keeps counting. A variable only so tests can use a small bound.
var maxSuppressedPerSummary = 1_000_000

type noRouteWindow struct {
	// last is the payload of the latest suppressed failure.
	last       any
	suppressed int
	flush      func(last any, suppressed int)
}

type noRouteMerger struct {
	mu      sync.Mutex
	windows map[string]*noRouteWindow
	limit   int
	window  time.Duration
	after   func(time.Duration, func())
}

var defaultNoRouteMerger = newNoRouteMerger(noRouteMergeLimit, noRouteMergeWindow)

func newNoRouteMerger(limit int, window time.Duration) *noRouteMerger {
	return &noRouteMerger{
		windows: map[string]*noRouteWindow{},
		limit:   limit,
		window:  window,
		after:   func(d time.Duration, f func()) { time.AfterFunc(d, f) },
	}
}

// NoteAllRefused records that hop selection for the request in ctx found no
// candidate because the per-destination refusals excluded every node. Only
// such failures are merged; other exclusions (bypass, matchers, an empty
// plan) are reported one by one.
func NoteAllRefused(ctx context.Context) {
	if a, _ := ctx.Value(attemptsKey{}).(*attempts); a != nil {
		a.mu.Lock()
		a.allRefused = true
		a.mu.Unlock()
	}
}

// AllRefused reports whether NoteAllRefused was called for ctx's request.
func AllRefused(ctx context.Context) bool {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.allRefused
}

// Refused reports whether the per-destination refusals exclude node for
// network/address.
func Refused(ctx context.Context, node *chain.Node, network, address string) bool {
	id := nodeRouteKey(node)
	return Enabled && id != (routeKey{}) && Tracking(ctx) && defaultRefusals.contains(id, network, address, time.Now())
}

// MergeNoRoute is called for a request that failed with no route before any
// attempt because every route refused the destination (AllRefused). It
// reports whether the caller should report the failure now: true for the
// first failure for network/address in a window (or when merging is not
// possible), false when the failure was counted into the open window.
// payload describes this failure. When a window that suppressed failures
// closes, flush runs once with the latest suppressed payload and the number
// of suppressed failures.
func MergeNoRoute(network, address string, payload any, flush func(last any, suppressed int)) bool {
	if !Enabled || !Escalation {
		return true
	}
	return defaultNoRouteMerger.merge(network, address, payload, flush)
}

func (m *noRouteMerger) merge(network, address string, payload any, flush func(any, int)) bool {
	key, ok := refusalKey(routeKey{managedID: "-"}, network, address)
	if !ok || flush == nil {
		return true
	}
	id := key.network + "/" + key.address
	m.mu.Lock()
	if window := m.windows[id]; window != nil {
		window.last = payload
		window.suppressed++
		if window.suppressed < maxSuppressedPerSummary {
			m.mu.Unlock()
			return false
		}
		last, suppressed := window.last, window.suppressed
		window.suppressed = 0
		m.mu.Unlock()
		window.flush(last, suppressed)
		return false
	}
	defer m.mu.Unlock()
	if len(m.windows) >= m.limit {
		return true
	}
	window := &noRouteWindow{flush: flush}
	m.windows[id] = window
	m.after(m.window, func() { m.close(id, window) })
	return true
}

// close ends window. A late timer never closes a newer window for the same
// destination.
func (m *noRouteMerger) close(id string, window *noRouteWindow) {
	m.mu.Lock()
	if m.windows[id] != window {
		m.mu.Unlock()
		return
	}
	delete(m.windows, id)
	m.mu.Unlock()
	if window.suppressed > 0 {
		window.flush(window.last, window.suppressed)
	}
}
