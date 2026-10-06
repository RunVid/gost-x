package pineroute

import (
	"sync"
	"time"
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

// MergeNoRoute is called for a request that failed with no route before any
// attempt. It reports whether the caller should report the failure now: true
// for the first failure for network/address in a window (or when merging is
// not possible), false when the failure was counted into the open window.
// payload describes this failure. When a window that suppressed failures
// closes, flush runs once with the latest suppressed payload and the number
// of suppressed failures.
func MergeNoRoute(network, address string, payload any, flush func(last any, suppressed int)) bool {
	if !Enabled {
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
	m.windows[id] = &noRouteWindow{flush: flush}
	m.after(m.window, func() { m.close(id) })
	return true
}

func (m *noRouteMerger) close(id string) {
	m.mu.Lock()
	window := m.windows[id]
	delete(m.windows, id)
	m.mu.Unlock()
	if window != nil && window.suppressed > 0 {
		window.flush(window.last, window.suppressed)
	}
}
