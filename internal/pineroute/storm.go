package pineroute

import (
	"context"
	"sync"
	"time"

	"github.com/go-gost/core/chain"
)

// When every route of a destination is excluded by its per-destination
// refusals, each browser retry fails at once with no route. The first such
// failure in a window is reported as usual; the rest are counted and reported
// once, with the count, when the window closes.
const (
	noRouteMergeWindow = 10 * time.Second
	noRouteMergeLimit  = 1024
)

type noRouteWindow struct {
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
// candidate because the per-destination refusals excluded every node.
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

// MergeNoRoute reports whether a no-route failure for network/address should
// be reported now: true for the first in a window, false when it was counted
// into the open window. When such a window closes, flush runs once with the
// latest suppressed payload and the count.
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
	defer m.mu.Unlock()
	if window := m.windows[id]; window != nil {
		window.last = payload
		window.suppressed++
		return false
	}
	if len(m.windows) >= m.limit {
		return true
	}
	window := &noRouteWindow{flush: flush}
	m.windows[id] = window
	m.after(m.window, func() { m.close(id, window) })
	return true
}

// close ends window unless a newer window replaced it.
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
