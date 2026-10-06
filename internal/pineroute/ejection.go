package pineroute

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/gosocks5"
	mdutil "github.com/go-gost/x/metadata/util"
)

// Route ejection follows Envoy's outlier detection: a route with route-level
// evidence against it is ejected for min(base × k, max) plus jitter, k grows
// by one per ejection and shrinks by one per healthy window, and no more than
// half of a hop's managed routes are ejected at once. Ejection is a soft
// avoid: selection prefers the other routes and still uses an ejected one
// when nothing else is usable.
const (
	defaultEjectionBase = 30 * time.Second
	// ejectionMaxFactor caps the ejection time at 20 × base (10 min).
	ejectionMaxFactor = 20
	// healthyWindowFactor sets the healthy window to 2 × base (60 s).
	healthyWindowFactor = 2
	// maxEjectionK stops k once base × k reaches the cap, so decay is not
	// wasted on multiples nobody can observe.
	maxEjectionK = ejectionMaxFactor
	// ejectionBaseEnvironment shortens every ejection period for real-binary
	// contract tests. The session coordinator never sets it.
	ejectionBaseEnvironment = "PINE_GOST_EJECTION_BASE"
)

// Escalation turns on #637's route-health rules: escalating per-destination
// TTLs, escalating ejection with a cap, route-wide ejection, panic selection
// and no_route merging. The session coordinator sets PINE_GOST_ROUTE_EJECTION
// from the deployment's chart value. Without it GOST keeps the #7/#8 rules: fixed
// TTLs, a fixed 30 s differential ejection without cap, no panic and no
// merging. Route events are emitted either way.
var Escalation = os.Getenv(escalationEnvironment) == "on"

const escalationEnvironment = "PINE_GOST_ROUTE_EJECTION"

// Ejection reasons. They are bounded: the coordinator uses them as a metric
// label.
const (
	ReasonDestinationFailures = "destination_failures"
	ReasonTimeout             = "timeout"
	ReasonAuth                = "auth"
	ReasonProxyFailure        = "proxy_failure"
	ReasonConnectError        = "connect_error"
)

// Route event kinds.
const (
	RouteEjected  = "route_ejected"
	RouteRestored = "route_restored"
)

// RouteEvent reports a route ejection or restoration.
type RouteEvent struct {
	Kind         string
	RouteID      string
	SourceListID string
	Tier         string
	Reason       string
	// K is the ejection multiplier the ejection used.
	K int
	// TTL is the ejection time including jitter.
	TTL time.Duration
}

var routeEventSink atomic.Pointer[func(RouteEvent)]

// SetRouteEventSink installs the receiver of route events. internal/pineevent
// installs it; pineroute cannot import pineevent.
func SetRouteEventSink(sink func(RouteEvent)) {
	if sink == nil {
		routeEventSink.Store(nil)
		return
	}
	routeEventSink.Store(&sink)
}

func emitRouteEvent(event RouteEvent) {
	if sink := routeEventSink.Load(); sink != nil {
		(*sink)(event)
	}
}

func ejectionBaseFromEnvironment() time.Duration {
	if value := os.Getenv(ejectionBaseEnvironment); value != "" {
		if base, err := time.ParseDuration(value); err == nil && base > 0 && base <= defaultEjectionBase {
			return base
		}
	}
	return defaultEjectionBase
}

// routeInfo is the route identity carried on route events.
type routeInfo struct {
	sourceListID string
	tier         string
}

func nodeRouteInfo(node *chain.Node) routeInfo {
	md := node.Options().Metadata
	if md == nil {
		return routeInfo{}
	}
	sourceListID, _ := md.Get("pine_source_list_id").(string)
	tier, _ := md.Get("pine_tier").(string)
	return routeInfo{sourceListID: sourceListID, tier: tier}
}

type ejectionRecord struct {
	k int
	// until is when the current ejection ends; zero when not ejected.
	until time.Time
	ttl   time.Duration
	// reason is the reason of the latest ejection.
	reason string
	// lastBad is the latest evidence against the route or its latest
	// restoration; healthy windows are counted from it.
	lastBad time.Time
	// generation identifies the ejection a restore timer belongs to.
	generation uint64
	info       routeInfo
}

type ejectionStore struct {
	mu         sync.Mutex
	entries    map[incarnation]*ejectionRecord
	limit      int
	generation uint64

	base    time.Duration
	now     func() time.Time
	after   func(time.Duration, func())
	jitter  func(time.Duration) time.Duration
	emit    func(RouteEvent)
	healthy time.Duration
	max     time.Duration
}

var defaultEjections = newEjectionStore(refusalCacheLimit, ejectionBaseFromEnvironment())

func newEjectionStore(limit int, base time.Duration) *ejectionStore {
	return &ejectionStore{
		entries: map[incarnation]*ejectionRecord{},
		limit:   limit,
		base:    base,
		max:     ejectionMaxFactor * base,
		healthy: healthyWindowFactor * base,
		now:     time.Now,
		after:   func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		jitter: func(span time.Duration) time.Duration {
			if span <= 0 {
				return 0
			}
			return rand.N(span)
		},
		emit: emitRouteEvent,
	}
}

// recordLocked returns route's record, creating it if needed, or nil when
// the store is full of active ejections: those are never dropped early.
func (s *ejectionStore) recordLocked(route incarnation, now time.Time) *ejectionRecord {
	if record := s.entries[route]; record != nil {
		return record
	}
	if len(s.entries) >= s.limit && !s.evictLocked(now) {
		return nil
	}
	record := &ejectionRecord{}
	s.entries[route] = record
	return record
}

// evictLocked drops the record of a route that is not ejected and has the
// oldest evidence. It reports false when every record is an active ejection.
func (s *ejectionStore) evictLocked(now time.Time) bool {
	var victim incarnation
	var victimRecord *ejectionRecord
	for route, record := range s.entries {
		if record.until.After(now) {
			continue
		}
		if victimRecord == nil || record.lastBad.Before(victimRecord.lastBad) {
			victim, victimRecord = route, record
		}
	}
	if victimRecord == nil {
		return false
	}
	delete(s.entries, victim)
	return true
}

// noteBad records evidence against route that did not eject it, so the
// healthy-window clock restarts.
func (s *ejectionStore) noteBad(route incarnation) {
	if s.limit <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if record := s.recordLocked(route, now); record != nil {
		if !record.until.After(now) {
			s.decayLocked(record, now)
		}
		record.lastBad = now
	}
}

// decayLocked takes one off k per healthy window completed since the last
// evidence or the end of the last ejection, and restarts the count from now
// so a window is never applied twice.
func (s *ejectionStore) decayLocked(record *ejectionRecord, now time.Time) {
	if record.k == 0 || s.healthy <= 0 {
		return
	}
	since := record.lastBad
	if record.until.After(since) {
		since = record.until
	}
	if windows := int(now.Sub(since) / s.healthy); windows > 0 {
		record.k = max(0, record.k-windows)
	}
}

// eject ejects route unless it is already ejected or ejecting it would eject
// more than half of the managed routes in hop. It reports whether the route
// was ejected.
func (s *ejectionStore) eject(route incarnation, info routeInfo, hop []incarnation, reason string) bool {
	if s.limit <= 0 {
		return false
	}
	s.mu.Lock()
	now := s.now()
	record := s.recordLocked(route, now)
	if record == nil {
		s.mu.Unlock()
		return false
	}
	if record.until.After(now) || Escalation && !s.withinCapLocked(route, hop, now) {
		record.lastBad = now
		s.mu.Unlock()
		return false
	}
	var ttl time.Duration
	if Escalation {
		s.decayLocked(record, now)
		record.k = min(record.k+1, maxEjectionK)
		ttl = min(time.Duration(record.k)*s.base, s.max)
		ttl += s.jitter(ttl / 10)
	} else {
		// #8: a fixed ejection, no cap.
		record.k, ttl = 1, s.base
	}
	s.generation++
	record.until, record.ttl, record.reason, record.lastBad = now.Add(ttl), ttl, reason, now
	record.generation, record.info = s.generation, info
	event := s.eventLocked(RouteEjected, route, record)
	generation := record.generation
	s.mu.Unlock()

	s.emit(event)
	s.after(ttl, func() { s.restore(route, generation) })
	return true
}

// withinCapLocked reports whether route may be ejected: it belongs to hop and
// at most floor(M/2) of hop's M managed routes would then be ejected.
func (s *ejectionStore) withinCapLocked(route incarnation, hop []incarnation, now time.Time) bool {
	member, ejected := false, 0
	for _, other := range hop {
		if other == route {
			member = true
			continue
		}
		if record := s.entries[other]; record != nil && record.until.After(now) {
			ejected++
		}
	}
	return member && ejected+1 <= len(hop)/2
}

func (s *ejectionStore) restore(route incarnation, generation uint64) {
	s.mu.Lock()
	record := s.entries[route]
	if record == nil || record.generation != generation || record.until.IsZero() {
		s.mu.Unlock()
		return
	}
	now := s.now()
	if now.After(record.lastBad) {
		record.lastBad = now
	}
	record.until = time.Time{}
	event := s.eventLocked(RouteRestored, route, record)
	s.mu.Unlock()
	s.emit(event)
}

func (s *ejectionStore) eventLocked(kind string, route incarnation, record *ejectionRecord) RouteEvent {
	return RouteEvent{
		Kind: kind, RouteID: route.id, SourceListID: record.info.sourceListID, Tier: record.info.tier,
		Reason: record.reason, K: record.k, TTL: record.ttl,
	}
}

func (s *ejectionStore) ejected(route incarnation, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.entries[route]
	return record != nil && record.until.After(now)
}

// NoteHop records the managed routes of the hop selecting for the request in
// ctx. The ejection cap is counted against them.
func NoteHop(ctx context.Context, nodes []*chain.Node) {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return
	}
	var hop []incarnation
	for _, node := range nodes {
		if route, ok := managedIncarnation(node); ok {
			hop = append(hop, route)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hops == nil {
		a.hops = map[incarnation][]incarnation{}
	}
	for _, route := range hop {
		a.hops[route] = hop
	}
}

func hopOf(ctx context.Context, route incarnation) []incarnation {
	a, _ := ctx.Value(attemptsKey{}).(*attempts)
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hops[route]
}

// RecordRouteFailure ejects node for a failure that counts against the whole
// route: the cases in which the selector marks the node failed. The
// selector's fixed cooldown still applies; ejection extends the avoidance
// with escalating periods.
func RecordRouteFailure(ctx context.Context, node *chain.Node, network string, err error) {
	if !Enabled || !Escalation || err == nil || IgnoreFailure(ctx, node, network, err) {
		return
	}
	route, ok := managedIncarnation(node)
	if !ok {
		return
	}
	defaultEjections.eject(route, nodeRouteInfo(node), hopOf(ctx, route), routeFailureReason(err))
}

func routeFailureReason(err error) string {
	var reply socks5ReplyError
	switch {
	case errors.Is(err, gosocks5.ErrAuthFailure) || errors.Is(err, gosocks5.ErrBadMethod):
		return ReasonAuth
	case errors.Is(err, context.DeadlineExceeded):
		return ReasonTimeout
	case errors.As(err, &reply):
		return ReasonProxyFailure
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return ReasonTimeout
	}
	return ReasonConnectError
}

// Ejected reports whether node is a managed route that is currently ejected.
func Ejected(node *chain.Node) bool {
	route, ok := managedIncarnation(node)
	return Enabled && ok && defaultEjections.ejected(route, time.Now())
}

// WithoutEjected returns nodes minus ejected managed routes, or nil if none
// is ejected. Callers select from the result first and fall back to the full
// set, so ejection never removes the last usable route.
func WithoutEjected(nodes []*chain.Node) []*chain.Node {
	if !Enabled {
		return nil
	}
	now := time.Now()
	kept := make([]*chain.Node, 0, len(nodes))
	excluded := false
	for _, node := range nodes {
		if route, ok := managedIncarnation(node); ok && defaultEjections.ejected(route, now) {
			excluded = true
			continue
		}
		kept = append(kept, node)
	}
	if !excluded {
		return nil
	}
	return kept
}

// PanicSelect picks a node when the selector found none usable because every
// candidate is ejected or cooling down. Trying a cooled route beats failing
// the request with no route. Order: primary before backup, non-ejected
// before ejected within each, then hop order; a direct node only when no
// managed route is left. Candidates were already filtered for this request
// (tried routes and per-destination refusals), and an explicit-country hop
// has no direct node, so this never adds a direct exit there.
func PanicSelect(nodes []*chain.Node) *chain.Node {
	if !Enabled || !Escalation {
		return nil
	}
	now := time.Now()
	var best *chain.Node
	bestRank := 5
	for _, node := range nodes {
		if node == nil {
			continue
		}
		rank := 4
		if route, ok := managedIncarnation(node); ok {
			rank = 0
			if backupNode(node) {
				rank += 2
			}
			if defaultEjections.ejected(route, now) {
				rank++
			}
		}
		if rank < bestRank {
			best, bestRank = node, rank
		}
	}
	return best
}

func backupNode(node *chain.Node) bool {
	md := node.Options().Metadata
	return md != nil && mdutil.GetBool(md, "backup")
}

// SetTimingForTest replaces the ejection base (max and healthy window scale
// with it) and the no-route merge window until the returned function runs.
// Only tests in this module use it.
func SetTimingForTest(ejectionBase, mergeWindow time.Duration) (restore func()) {
	previousEjections, previousMerger := defaultEjections, defaultNoRouteMerger
	defaultEjections = newEjectionStore(refusalCacheLimit, ejectionBase)
	defaultNoRouteMerger = newNoRouteMerger(noRouteMergeLimit, mergeWindow)
	return func() { defaultEjections, defaultNoRouteMerger = previousEjections, previousMerger }
}
