package pineroute

// One sticky route per Computer: the hop keeps a current route that serves
// every site. Sticky only orders the candidates (current route first, then
// the routes after it, Chrome's timezone ahead of other routes); the hop's
// filters, #10's ejection and panic selection still decide. The current route
// changes only when it cannot serve a request route-wide (removed, outside
// Chrome's timezone, ejected, cooling down after a route failure) and a
// route in Chrome's timezone connects instead. Unlike plain fifo the hop does
// not return to the old route when its cooldown ends.

import (
	"context"
	"sort"
	"sync"

	"github.com/go-gost/core/chain"
)

// Reasons carried by route_moved events.
const (
	MoveRouteFailed  = "route_failed"
	MoveRouteEjected = "route_ejected"
	MoveRouteRemoved = "route_removed"
)

// Sticky is a hop's sticky-route configuration, rendered by the session
// coordinator into the hop's metadata so it changes with the routes on a
// config reload.
type Sticky struct {
	// timeZone is Chrome's timezone: only a route whose pine_route_tz equals
	// it becomes the current route.
	timeZone string
	state    *stickyState
}

// stickyState is a hop's current route. It outlives config reloads (keyed by
// hop name). Only requests selected under the latest configuration (config)
// and the latest current route (generation, bumped by every promotion) can
// promote.
type stickyState struct {
	mu         sync.Mutex
	route      string
	generation uint64
	config     *Sticky
}

var stickyStates sync.Map // hop name -> *stickyState

// NewSticky returns the sticky-route configuration for hop, or nil when GOST
// does not run under Pine or Chrome's timezone is unknown (plain fifo). Each
// call is a config load: requests selected under an older one, which may
// have seen other routes, cannot promote.
func NewSticky(hop, timeZone string) *Sticky {
	if !Enabled || timeZone == "" {
		return nil
	}
	v, _ := stickyStates.LoadOrStore(hop, &stickyState{})
	state := v.(*stickyState)
	sticky := &Sticky{timeZone: timeZone, state: state}
	state.mu.Lock()
	state.config = sticky
	state.mu.Unlock()
	return sticky
}

// DisableSticky is a config load of hop without a sticky route: requests
// selected under an earlier load can no longer promote.
func DisableSticky(hop string) {
	v, ok := stickyStates.Load(hop)
	if !ok {
		return
	}
	state := v.(*stickyState)
	state.mu.Lock()
	state.config = nil
	state.mu.Unlock()
}

// Current returns the hop's current route ID ("" before the first success).
func (s *Sticky) Current() string {
	route, _ := s.state.get()
	return route
}

func (st *stickyState) get() (string, uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.route, st.generation
}

func (st *stickyState) swap(config *Sticky, generation uint64, route string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.config != config || st.generation != generation {
		return false
	}
	st.route = route
	st.generation++
	return true
}

// inTimeZone reports whether node is a managed route whose exit is in
// Chrome's timezone: the only routes that become current.
func (s *Sticky) inTimeZone(node *chain.Node) bool {
	if managedRouteID(node) == "" || node.Options().Metadata == nil {
		return false
	}
	zone, _ := node.Options().Metadata.Get("pine_route_tz").(string)
	return zone == s.timeZone
}

func managedRouteID(node *chain.Node) string {
	if node == nil || IsDirect(node) {
		return ""
	}
	return nodeRouteKey(node).managedID
}

// StickySelection is the sticky state of one router dial: the current route
// it started from and why it may leave it. A dial's attempts run one after
// another, so it needs no lock.
type StickySelection struct {
	sticky *Sticky
	// current is the hop's current route, or before the first success the
	// plan's first route in Chrome's timezone; stored is the state's.
	current, stored string
	generation      uint64
	reason          string
	// lastResort holds the routes this request picked as a last resort; they
	// never become current.
	lastResort map[string]bool
}

// StickyFor returns the sticky state of the request in ctx, creating it on
// the first selection. Nil without sticky configuration or tracking.
func StickyFor(ctx context.Context, network string, sticky *Sticky) *StickySelection {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if sticky == nil || a == nil || !tcpNetwork(network) {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sticky == nil {
		route, generation := sticky.state.get()
		a.sticky = &StickySelection{sticky: sticky, current: route, stored: route, generation: generation}
	}
	return a.sticky
}

func stickyFromContext(ctx context.Context) *StickySelection {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sticky
}

// Order records why the request cannot use the current route, if it cannot,
// and returns candidates in sticky order: managed routes in Chrome's
// timezone, then other managed routes, then the rest; each group in plan
// order rotated to start at the current route. cooling reports whether the
// hop's filters hold a route back (cooldown after a route failure).
func (s *StickySelection) Order(all, candidates []*chain.Node, cooling func(*chain.Node) bool) []*chain.Node {
	if s == nil {
		return candidates
	}
	if s.current == "" {
		// Before the first success the plan's first route in Chrome's
		// timezone is current: a refusal there is a detour too.
		for _, node := range all {
			if s.sticky.inTimeZone(node) {
				s.current = managedRouteID(node)
				break
			}
		}
	}
	start := -1
	for i, node := range all {
		if s.current != "" && managedRouteID(node) == s.current {
			start = i
			s.judge(node, cooling)
		}
	}
	if s.reason == MoveRouteRemoved {
		// Gone from the plan or from Chrome's timezone: plan order.
		start = -1
	}
	if s.current != "" && start < 0 {
		s.note(MoveRouteRemoved)
	}
	index := make(map[*chain.Node]int, len(all))
	for i, node := range all {
		index[node] = i
	}
	rank := func(node *chain.Node) (int, int) {
		group, pos := 2, index[node]
		if managedRouteID(node) != "" {
			group = 1
			if s.sticky.inTimeZone(node) {
				group = 0
			}
			if start >= 0 {
				pos = (pos - start + len(all)) % len(all)
			}
		}
		return group, pos
	}
	ordered := append([]*chain.Node(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		gi, pi := rank(ordered[i])
		gj, pj := rank(ordered[j])
		return gi < gj || gi == gj && pi < pj
	})
	return ordered
}

// judge records whether the current route cannot serve route-wide. Ejected
// includes #8's quarantine when #10's escalation is off. A
// refusal for this destination or a host the route is not offered for is a
// detour and leaves the current route in place.
func (s *StickySelection) judge(current *chain.Node, cooling func(*chain.Node) bool) {
	switch {
	case !s.sticky.inTimeZone(current):
		s.note(MoveRouteRemoved)
	case Ejected(current):
		s.note(MoveRouteEjected)
	case cooling(current):
		s.note(MoveRouteFailed)
	}
}

// note keeps the first reason, except that an ejection is always reported as
// such.
func (s *StickySelection) note(reason string) {
	if s.reason == "" || reason == MoveRouteEjected {
		s.reason = reason
	}
}

// MarkLastResort records that node was picked as a last resort: a panic
// pick, an ejected route, or a lone candidate the filters hold back.
func (s *StickySelection) MarkLastResort(node *chain.Node) {
	if s == nil || managedRouteID(node) == "" {
		return
	}
	if s.lastResort == nil {
		s.lastResort = map[string]bool{}
	}
	s.lastResort[managedRouteID(node)] = true
}

// StickyMove describes a change of the current route, for route_moved.
type StickyMove struct {
	FromRoute string
	ToRoute   string
	Reason    string
}

// StickySuccess updates the current route after winner connected: winner
// replaces it only when the request had a reason to leave it, winner is in
// Chrome's timezone and not a last resort, and the state is still the one
// the request started from. The change is returned for an event.
func StickySuccess(ctx context.Context, winner *chain.Node) *StickyMove {
	s := stickyFromContext(ctx)
	if s == nil {
		return nil
	}
	route := managedRouteID(winner)
	if s.lastResort[route] {
		return nil
	}
	if route != "" && route == s.current && s.stored == "" {
		// The plan's first route served: it becomes the stored current route.
		s.sticky.state.swap(s.sticky, s.generation, route)
		return nil
	}
	if route == s.current || s.current == "" || s.reason == "" || !s.sticky.inTimeZone(winner) {
		return nil
	}
	if !s.sticky.state.swap(s.sticky, s.generation, route) {
		return nil
	}
	return &StickyMove{FromRoute: s.current, ToRoute: route, Reason: s.reason}
}
