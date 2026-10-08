package hop

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-gost/core/bypass"
	corechain "github.com/go-gost/core/chain"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
	mdx "github.com/go-gost/x/metadata"
	xselector "github.com/go-gost/x/selector"
)

// chromeTZ is the timezone Chrome runs with in these tests.
const chromeTZ = "America/New_York"

var stickySeq atomic.Int64

// stickyByHop holds the latest sticky configuration per hop name, to read
// its current route.
var (
	stickyMu    sync.Mutex
	stickyByHop = map[string]*pineroute.Sticky{}
)

func uniqueID(prefix string) string {
	return fmt.Sprintf("er_%s_%d_%d", prefix, time.Now().UnixNano(), stickySeq.Add(1))
}

func uniqueSite(prefix string) string {
	return fmt.Sprintf("%s-%d-%d.example", prefix, time.Now().UnixNano(), stickySeq.Add(1))
}

// stickyRouter is newTestRouter with the sticky route on hop, as the
// coordinator renders it. Routers built with the same hop name share the
// current route, like reloads of one GOST.
func stickyRouter(t *testing.T, hop, zone string, nodes ...*corechain.Node) *xchain.Router {
	t.Helper()
	sticky := pineroute.NewSticky(hop, zone)
	stickyMu.Lock()
	stickyByHop[hop] = sticky
	stickyMu.Unlock()
	h := NewHop(NodeOption(nodes...), StickyOption(sticky),
		SelectorOption(xselector.NewSelector(
			xselector.FIFOStrategy[*corechain.Node](),
			xselector.FailFilter[*corechain.Node](1, 30*time.Second),
			xselector.BackupFilter[*corechain.Node](),
		)))
	c := xchain.NewChain("provider-routes")
	c.AddHop(h)
	return xchain.NewRouter(
		corechain.ChainRouterOption(c),
		corechain.RetriesRouterOption(len(nodes)-1),
		corechain.LoggerRouterOption(xlogger.Nop()),
	)
}

// route is a managed primary route whose exit is in zone ("" = unknown).
func route(id, zone string, transport corechain.Transporter, extra ...corechain.NodeOption) *corechain.Node {
	return routeWith(id, zone, transport, nil, extra...)
}

func routeWith(id, zone string, transport corechain.Transporter, md map[string]any, extra ...corechain.NodeOption) *corechain.Node {
	all := map[string]any{"pine_route_id": id, "pine_tier": "primary"}
	for k, v := range md {
		all[k] = v
	}
	if zone != "" {
		all["pine_route_tz"] = zone
	}
	opts := append([]corechain.NodeOption{corechain.TransportNodeOption(transport), corechain.MetadataNodeOption(mdx.NewMetadata(all))}, extra...)
	return corechain.NewNode(id, id+".example:1080", opts...)
}

// movedEvents captures one test's route_moved events.
type movedEvents struct {
	mu   sync.Mutex
	list []pineevent.Event
}

func captureMoves(t *testing.T) *movedEvents {
	t.Helper()
	events := &movedEvents{}
	restore := pineevent.CaptureForTest(func(event pineevent.Event) {
		if event.Kind == "route_moved" {
			events.mu.Lock()
			events.list = append(events.list, event)
			events.mu.Unlock()
		}
	})
	t.Cleanup(restore)
	return events
}

func (m *movedEvents) all() []pineevent.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]pineevent.Event(nil), m.list...)
}

type hostBypass struct{ hosts map[string]bool }

func (b hostBypass) IsWhitelist() bool { return false }
func (b hostBypass) Contains(_ context.Context, _, addr string, opts ...bypass.Option) bool {
	var options bypass.Options
	for _, opt := range opts {
		opt(&options)
	}
	host := options.Host
	if host == "" {
		host = addr
	}
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return b.hosts[host]
}

// whitelist bypasses every host it does not list, like the ISP login-site
// whitelist the coordinator renders.
type whitelist struct{ hostBypass }

func (w whitelist) IsWhitelist() bool { return true }
func (w whitelist) Contains(ctx context.Context, network, addr string, opts ...bypass.Option) bool {
	return !w.hostBypass.Contains(ctx, network, addr, opts...)
}

func mustDial(t *testing.T, r *xchain.Router, host string) {
	t.Helper()
	if err := dial(t, r, host+":443"); err != nil {
		t.Fatalf("%s: %v", host, err)
	}
}

// visit dials one fresh site per call.
func visit(t *testing.T, r *xchain.Router, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mustDial(t, r, "www."+uniqueSite("s"))
	}
}

func counts(ts ...*refusingTransport) []int32 {
	out := make([]int32, len(ts))
	for i, tr := range ts {
		out[i] = tr.connects.Load()
	}
	return out
}

func wantCounts(t *testing.T, what string, got []int32, want ...int32) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s: connects = %v, want %v", what, got, want)
	}
}

// failRoute makes CONNECTs through tr fail route-wide (general failure at
// the handshake, which cools the route down as on main). A failed handshake
// never reaches Connect, so connects counts served and refused CONNECTs.
func failRoute(tr *refusingTransport) { tr.handshakeErr = socksReply(1) }

// recover ends a route's failure and its cooldown.
func recoverRoute(tr *refusingTransport, node *corechain.Node) {
	tr.handshakeErr = nil
	node.Marker().Reset()
}

// noEscalation turns #10's ejection off, as in production without
// PINE_GOST_ROUTE_EJECTION: route failures only cool routes down.
func noEscalation(t *testing.T) {
	t.Cleanup(pineroute.SetEscalationForTest(false))
}

// Without a failure every site uses the first route.
func TestStickyStartsOnFirstRoute(t *testing.T) {
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	visit(t, r, 5)
	wantCounts(t, "start", counts(ta, tb), 5, 0)
}

// Plain fifo (sticky off) returns to the first route when its cooldown ends.
func TestWithoutStickyFIFOFlipsBack(t *testing.T) {
	noEscalation(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	a := route(uniqueID("a"), chromeTZ, ta)
	r := newTestRouter(a, route(uniqueID("b"), chromeTZ, tb))
	visit(t, r, 1)
	failRoute(ta)
	visit(t, r, 1)
	recoverRoute(ta, a)
	visit(t, r, 1)
	wantCounts(t, "fifo", counts(ta, tb), 2, 1)
}

// A route failure moves the Computer to the next route, which it keeps when
// the first route recovers; it moves again only when that route fails.
func TestStickyMovesOnRouteFailureAndStays(t *testing.T) {
	noEscalation(t)
	events := captureMoves(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	idA, idB := uniqueID("a"), uniqueID("b")
	a, b := route(idA, chromeTZ, ta), route(idB, chromeTZ, tb)
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, b)
	visit(t, r, 1)
	failRoute(ta)
	visit(t, r, 1) // a fails, b serves and becomes current
	recoverRoute(ta, a)
	visit(t, r, 3)
	wantCounts(t, "after recovery", counts(ta, tb), 1, 4)
	moved := events.all()
	if len(moved) != 1 || moved[0].FromRouteID != idA || moved[0].RouteID != idB || moved[0].Reason != pineroute.MoveRouteFailed {
		t.Fatalf("events = %+v", moved)
	}
	failRoute(tb)
	visit(t, r, 1) // b fails: back to a, which is the next route after b
	recoverRoute(tb, b)
	visit(t, r, 2)
	wantCounts(t, "second move", counts(ta, tb), 4, 4)
	if moved = events.all(); len(moved) != 2 || moved[1].FromRouteID != idB || moved[1].RouteID != idA {
		t.Fatalf("events = %+v", moved)
	}
}

// When the current route fails the Computer moves to the route after it,
// not back to the first route.
func TestStickyRotatesAfterCurrentRoute(t *testing.T) {
	noEscalation(t)
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	a, b := route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb)
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, b, route(uniqueID("c"), chromeTZ, tc))
	visit(t, r, 1)
	failRoute(ta)
	visit(t, r, 1) // -> b
	recoverRoute(ta, a)
	failRoute(tb)
	visit(t, r, 1) // b fails -> c, although a is healthy again
	recoverRoute(tb, b)
	visit(t, r, 2)
	wantCounts(t, "rotation", counts(ta, tb, tc), 1, 1, 3)
}

// The first request refused by the plan's first route is a detour too: that
// route is current from the start.
func TestStickyFirstRequestRefusalIsDetour(t *testing.T) {
	events := captureMoves(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	idA, hop := uniqueID("a"), uniqueID("hop")
	r := stickyRouter(t, hop, chromeTZ, route(idA, chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	site := uniqueSite("first")
	ta.refused = map[string]error{"login." + site: socksReply(5)}
	mustDial(t, r, "login."+site)
	visit(t, r, 2)
	wantCounts(t, "first request detour", counts(ta, tb), 3, 1)
	if moved := events.all(); len(moved) != 0 || stickyByHop[hop].Current() != idA {
		t.Fatalf("moves %+v, current %s", moved, stickyByHop[hop].Current())
	}
}

// A route failure on the very first request moves the Computer, reported
// from the plan's first route.
func TestStickyFirstRequestRouteFailureMoves(t *testing.T) {
	noEscalation(t)
	events := captureMoves(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	idA, idB := uniqueID("a"), uniqueID("b")
	a := route(idA, chromeTZ, ta)
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, route(idB, chromeTZ, tb))
	failRoute(ta)
	visit(t, r, 1)
	recoverRoute(ta, a)
	visit(t, r, 2)
	wantCounts(t, "first request failure", counts(ta, tb), 0, 3)
	if moved := events.all(); len(moved) != 1 || moved[0].FromRouteID != idA || moved[0].RouteID != idB {
		t.Fatalf("events = %+v", moved)
	}
}

// A refusal of one destination by the current route, or any other
// destination-scoped failure, is a detour: the current route stays.
func TestStickyRefusalIsDetour(t *testing.T) {
	events := captureMoves(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	visit(t, r, 1)
	site := uniqueSite("refused")
	ta.refused = map[string]error{"www." + site: socksReply(5), "api." + site: socksReply(4)}
	mustDial(t, r, "www."+site)
	mustDial(t, r, "api."+site)
	visit(t, r, 2)
	wantCounts(t, "detour", counts(ta, tb), 5, 2)
	if moved := events.all(); len(moved) != 0 {
		t.Fatalf("detour moved the current route: %+v", moved)
	}
}

// A route in another timezone serves when no route in Chrome's timezone can,
// but never becomes current: the Computer returns to its route when it
// recovers. A later route in Chrome's timezone is preferred over it.
func TestStickyOtherTimeZoneIsDetourOnly(t *testing.T) {
	noEscalation(t)
	t.Run("only other timezone left", func(t *testing.T) {
		events := captureMoves(t)
		ta, tb := &refusingTransport{}, &refusingTransport{}
		a := route(uniqueID("a"), chromeTZ, ta)
		r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, route(uniqueID("b"), "America/Los_Angeles", tb))
		visit(t, r, 1)
		failRoute(ta)
		visit(t, r, 1) // detour through b
		recoverRoute(ta, a)
		visit(t, r, 2)
		wantCounts(t, "detour", counts(ta, tb), 3, 1)
		if moved := events.all(); len(moved) != 0 {
			t.Fatalf("other-timezone route became current: %+v", moved)
		}
	})
	t.Run("same timezone later in the plan wins", func(t *testing.T) {
		ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
		a := route(uniqueID("a"), chromeTZ, ta)
		r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, route(uniqueID("b"), "America/Los_Angeles", tb), route(uniqueID("c"), chromeTZ, tc))
		visit(t, r, 1)
		failRoute(ta)
		visit(t, r, 1) // -> c, skipping the other-timezone b
		recoverRoute(ta, a)
		visit(t, r, 2)
		wantCounts(t, "same timezone", counts(ta, tb, tc), 1, 0, 3)
	})
}

// Tiers come first: an other-timezone primary serves before a fallback in
// Chrome's timezone, as a detour.
func TestStickyKeepsTierOrder(t *testing.T) {
	noEscalation(t)
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	a := route(uniqueID("a"), chromeTZ, ta)
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, a, route(uniqueID("b"), "America/Chicago", tb),
		routeWith(uniqueID("c"), chromeTZ, tc, map[string]any{"pine_tier": "fallback", "backup": true}))
	visit(t, r, 1)
	failRoute(ta)
	visit(t, r, 1)
	recoverRoute(ta, a)
	visit(t, r, 1)
	wantCounts(t, "tiers", counts(ta, tb, tc), 2, 1, 0)
}

// The current route cooling down alone is noticed (the filters judge a lone
// candidate too), and with every route cooling a panic pick serves without
// becoming current.
func TestStickyCooldownAndPanicPick(t *testing.T) {
	events := captureMoves(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	idA := uniqueID("a")
	hop := uniqueID("hop")
	r := stickyRouter(t, hop, chromeTZ, route(idA, chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	visit(t, r, 1)
	failRoute(ta)
	failRoute(tb)
	if err := dial(t, r, "www."+uniqueSite("down")+":443"); err == nil {
		t.Fatal("both routes failing, want an error")
	}
	// Both cool down; b works again before its cooldown ends, so the hop
	// finds no usable route: the panic pick a fails and b, the last
	// candidate, serves as a last resort. It does not become current.
	tb.handshakeErr = nil
	visit(t, r, 1)
	if got := tb.connects.Load(); got != 1 {
		t.Fatalf("b connects = %d, want the last resort served by b", got)
	}
	if moved := events.all(); len(moved) != 0 || stickyByHop[hop].Current() != idA {
		t.Fatalf("last resort became current: %+v, current %s", moved, stickyByHop[hop].Current())
	}
}

// The only route left in Chrome's timezone cooling down serves as a last
// resort after the current route left the plan, without becoming current.
func TestStickyLoneCoolingRouteIsLastResort(t *testing.T) {
	noEscalation(t)
	events := captureMoves(t)
	hop := uniqueID("hop")
	idA, idB := uniqueID("a"), uniqueID("b")
	ta, tb := &refusingTransport{}, &refusingTransport{}
	b := route(idB, chromeTZ, tb)
	r := stickyRouter(t, hop, chromeTZ, route(idA, chromeTZ, ta), b)
	visit(t, r, 1)                        // current a
	b.Marker().Mark()                     // b cooling down after a route failure
	r = stickyRouter(t, hop, chromeTZ, b) // a left the plan
	visit(t, r, 1)
	if got := tb.connects.Load(); got != 1 {
		t.Fatalf("b connects = %d, want the last resort served by b", got)
	}
	if moved := events.all(); len(moved) != 0 || stickyByHop[hop].Current() != idA {
		t.Fatalf("last resort became current: %+v, current %s", moved, stickyByHop[hop].Current())
	}
}

// Ejection (#10) of the current route moves the Computer.
func TestStickyEjectionMoves(t *testing.T) {
	events := captureMoves(t)
	idA, idB := uniqueID("a"), uniqueID("b")
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, route(idA, chromeTZ, ta), route(idB, chromeTZ, tb), route(uniqueID("c"), chromeTZ, tc))
	visit(t, r, 1)
	// Charge a: hosts it fails with reply 4 while b reaches them (detours).
	ta.refused = map[string]error{}
	for i := 0; i < escalationHosts; i++ {
		host := "q." + uniqueSite("eject")
		ta.refused[host] = socksReply(4)
		mustDial(t, r, host)
	}
	visit(t, r, 2)
	moved := events.all()
	if len(moved) != 1 || moved[0].FromRouteID != idA || moved[0].RouteID != idB || moved[0].Reason != pineroute.MoveRouteEjected {
		t.Fatalf("events = %+v", moved)
	}
	if got := tb.connects.Load(); got != escalationHosts+2 {
		t.Fatalf("b connects = %d", got)
	}
}

// A reload keeps the current route while the plan has it in Chrome's
// timezone; a plan without it, or with its exit elsewhere, moves the
// Computer in plan order.
func TestStickyPlanRefresh(t *testing.T) {
	noEscalation(t)
	for _, name := range []string{"kept", "removed", "timezone changed"} {
		t.Run(name, func(t *testing.T) {
			events := captureMoves(t)
			hop := uniqueID("hop")
			idA, idB, idC := uniqueID("a"), uniqueID("b"), uniqueID("c")
			ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
			a := route(idA, chromeTZ, ta)
			r := stickyRouter(t, hop, chromeTZ, a, route(idB, chromeTZ, tb))
			visit(t, r, 1)
			failRoute(ta)
			visit(t, r, 1) // current b
			recoverRoute(ta, a)
			nodes := []*corechain.Node{route(idA, chromeTZ, ta), route(idB, chromeTZ, tb), route(idC, chromeTZ, tc)}
			switch name {
			case "removed":
				nodes = []*corechain.Node{nodes[0], nodes[2]}
			case "timezone changed":
				nodes[1] = route(idB, "America/Denver", tb)
			}
			r = stickyRouter(t, hop, chromeTZ, nodes...)
			visit(t, r, 2)
			moved := events.all()
			if name == "kept" {
				wantCounts(t, name, counts(ta, tb, tc), 1, 3, 0)
				if len(moved) != 1 {
					t.Fatalf("events = %+v", moved)
				}
				return
			}
			// Gone from the plan or from Chrome's timezone: plan order (a).
			wantCounts(t, name, counts(ta, tb, tc), 3, 1, 0)
			if len(moved) != 2 || moved[1].FromRouteID != idB || moved[1].RouteID != idA || moved[1].Reason != pineroute.MoveRouteRemoved {
				t.Fatalf("events = %+v", moved)
			}
		})
	}
}

// Login hosts offered an ISP route keep the ISP tier and do not touch the
// current route, even when they end up on a managed route.
func TestStickyLeavesISPHostsAlone(t *testing.T) {
	noEscalation(t)
	events := captureMoves(t)
	tisp, ta, tb := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	login := "accounts." + uniqueSite("login")
	isp := routeWith(uniqueID("isp"), "", tisp, map[string]any{"pine_tier": "isp"},
		corechain.BypassNodeOption(whitelist{hostBypass{hosts: map[string]bool{login: true}}}))
	a := route(uniqueID("a"), chromeTZ, ta)
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, isp, a, route(uniqueID("b"), chromeTZ, tb))
	visit(t, r, 1)
	mustDial(t, r, login)
	wantCounts(t, "isp", counts(tisp, ta, tb), 1, 1, 0)
	// The ISP route refuses the login host and a fails: b serves it, which
	// would be a move for any other host.
	tisp.refused = map[string]error{login: socksReply(5)}
	failRoute(ta)
	mustDial(t, r, login)
	recoverRoute(ta, a)
	visit(t, r, 1)
	wantCounts(t, "isp host left current alone", counts(tisp, ta, tb), 2, 2, 1)
	if moved := events.all(); len(moved) != 0 {
		t.Fatalf("events = %+v", moved)
	}
}

// Concurrent requests that see the current route fail move it once.
func TestStickyConcurrentFailureMovesOnce(t *testing.T) {
	noEscalation(t)
	events := captureMoves(t)
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	r := stickyRouter(t, uniqueID("hop"), chromeTZ, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb), route(uniqueID("c"), chromeTZ, tc))
	visit(t, r, 1)
	failRoute(ta)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dial(t, r, "www."+uniqueSite("race")+":443")
		}()
	}
	wg.Wait()
	if moved := events.all(); len(moved) != 1 {
		t.Fatalf("events = %+v", moved)
	}
	if tc.connects.Load() != 0 {
		t.Fatal("a request skipped b")
	}
}

// Without Chrome's timezone or route timezones the hop is plain fifo.
func TestStickyWithoutTimeZoneIsFIFO(t *testing.T) {
	noEscalation(t)
	for _, name := range []string{"no route timezones", "no chrome timezone"} {
		t.Run(name, func(t *testing.T) {
			ta, tb := &refusingTransport{}, &refusingTransport{}
			zone, chrome := "", chromeTZ
			if name == "no chrome timezone" {
				zone, chrome = chromeTZ, ""
			}
			a := route(uniqueID("a"), zone, ta)
			r := stickyRouter(t, uniqueID("hop"), chrome, a, route(uniqueID("b"), zone, tb))
			visit(t, r, 1)
			failRoute(ta)
			visit(t, r, 1)
			recoverRoute(ta, a)
			visit(t, r, 1)
			wantCounts(t, name, counts(ta, tb), 2, 1)
		})
	}
}
