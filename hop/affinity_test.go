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
	"github.com/go-gost/core/routing"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
	mdx "github.com/go-gost/x/metadata"
	xselector "github.com/go-gost/x/selector"
)

// chromeTZ is the timezone Chrome runs with in these tests.
const chromeTZ = "America/New_York"

var affinitySeq atomic.Int64

// testLoginSites is the login-site list affinityRouter renders.
var testLoginSites []string

// affinityRouter is newTestRouter with per-site affinity on the hop, as the
// coordinator renders it.
func affinityRouter(t *testing.T, nodes ...*corechain.Node) *xchain.Router {
	t.Helper()
	return affinityRouterTZ(t, chromeTZ, nodes...)
}

func affinityRouterTZ(t *testing.T, zone string, nodes ...*corechain.Node) *xchain.Router {
	t.Helper()
	h := NewHop(NodeOption(nodes...), AffinityOption(pineroute.NewAffinity(zone, testLoginSites)),
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

// route is a managed node whose exit is in zone ("" = no timezone known).
func route(id, zone string, transport corechain.Transporter, extra ...corechain.NodeOption) *corechain.Node {
	md := map[string]any{"pine_route_id": id, "pine_tier": "primary"}
	if zone != "" {
		md["pine_route_tz"] = zone
	}
	opts := append([]corechain.NodeOption{corechain.TransportNodeOption(transport), corechain.MetadataNodeOption(mdx.NewMetadata(md))}, extra...)
	return corechain.NewNode(id, id+".example:1080", opts...)
}

// withAffinity captures one test's affinity_moved events.
func withAffinity(t *testing.T) *movedEvents {
	t.Helper()
	events := &movedEvents{}
	restore := pineevent.CaptureForTest(func(event pineevent.Event) {
		if event.Kind == "affinity_moved" {
			events.mu.Lock()
			events.list = append(events.list, event)
			events.mu.Unlock()
		}
	})
	t.Cleanup(restore)
	return events
}

type movedEvents struct {
	mu   sync.Mutex
	list []pineevent.Event
}

func (m *movedEvents) forSite(site string) []pineevent.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []pineevent.Event
	for _, event := range m.list {
		if event.Site == site {
			out = append(out, event)
		}
	}
	return out
}

func uniqueID(prefix string) string {
	return fmt.Sprintf("er_%s_%d_%d", prefix, time.Now().UnixNano(), affinitySeq.Add(1))
}

func uniqueSite(prefix string) string {
	return fmt.Sprintf("%s-%d-%d.example", prefix, time.Now().UnixNano(), affinitySeq.Add(1))
}

type loginBypass struct{ hosts map[string]bool }

func (b loginBypass) IsWhitelist() bool { return false }
func (b loginBypass) Contains(_ context.Context, _, addr string, opts ...bypass.Option) bool {
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
type whitelist struct{ loginBypass }

func (w whitelist) IsWhitelist() bool { return true }
func (w whitelist) Contains(ctx context.Context, network, addr string, opts ...bypass.Option) bool {
	return !w.loginBypass.Contains(ctx, network, addr, opts...)
}

// withLoginSites sets the login-site list of the routers a test builds.
func withLoginSites(t *testing.T, matchers ...string) {
	t.Helper()
	testLoginSites = matchers
	t.Cleanup(func() { testLoginSites = nil })
}

func mustDial(t *testing.T, r *xchain.Router, host string) {
	t.Helper()
	if err := dial(t, r, host+":443"); err != nil {
		t.Fatalf("%s: %v", host, err)
	}
}

// Without a failure every site stays on the first route.
func TestAffinityStartsOnFIFOOrder(t *testing.T) {
	withAffinity(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	for i := 0; i < 5; i++ {
		mustDial(t, r, "www."+uniqueSite("fifo"))
	}
	if ta.connects.Load() != 5 || tb.connects.Load() != 0 {
		t.Fatalf("a=%d b=%d, want every site on the first route", ta.connects.Load(), tb.connects.Load())
	}
}

// Affinity off: plain fifo.
func TestAffinityOffKeepsFIFO(t *testing.T) {
	ta, tb := &refusingTransport{}, &refusingTransport{}
	site := uniqueSite("off")
	ta.refused = map[string]error{"login." + site: socksReply(5)}
	r := newTestRouter(route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	mustDial(t, r, "login."+site)
	mustDial(t, r, "www."+site)
	// a sees the refused CONNECT and www; nothing was pinned to b.
	if ta.connects.Load() != 2 || tb.connects.Load() != 1 {
		t.Fatalf("a=%d b=%d, want fifo (no pin)", ta.connects.Load(), tb.connects.Load())
	}
}

// A site that fails over is pinned to the route that served it and stays
// there for its other hosts; it does not flip back to the first route.
// Other sites are unaffected.
func TestAffinityPinsAfterFailoverWithoutFlipBack(t *testing.T) {
	events := withAffinity(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb))
	site, other := uniqueSite("pin"), uniqueSite("other")
	ta.refused = map[string]error{"login." + site: socksReply(5)}
	mustDial(t, r, "login."+site)
	for _, host := range []string{"www.", "api.", "cdn."} {
		mustDial(t, r, host+site)
	}
	mustDial(t, r, "www."+other)
	// a: the refused CONNECT and the other site; b: the site's four hosts.
	if tb.connects.Load() != 4 || ta.connects.Load() != 2 {
		t.Fatalf("a=%d b=%d, want the site on b and the other site on a", ta.connects.Load(), tb.connects.Load())
	}
	if len(events.forSite(site)) != 0 {
		t.Fatalf("first pin reported as a move: %+v", events.forSite(site))
	}
}

// With ejection switched off a route failure only cools the first route:
// the site moves to the second route, is pinned there, and stays after the
// cooldown ends; a fresh site still starts on the first route.
func TestAffinityPinsAfterRouteCooldownWithoutFlipBack(t *testing.T) {
	withAffinity(t)
	pineroute.Escalation = false
	t.Cleanup(func() { pineroute.Escalation = true })
	ta, tb := &refusingTransport{}, &refusingTransport{}
	aNode := route(uniqueID("a"), chromeTZ, ta)
	r := affinityRouter(t, aNode, route(uniqueID("b"), chromeTZ, tb))
	site := uniqueSite("cool")
	mustDial(t, r, "www."+site)
	ta.handshakeErr = socksReply(1)
	mustDial(t, r, "www."+site)
	ta.handshakeErr = nil
	aNode.Marker().Reset()
	mustDial(t, r, "api."+site)
	mustDial(t, r, "www."+uniqueSite("fresh"))
	if tb.connects.Load() != 2 || ta.connects.Load() != 2 {
		t.Fatalf("a=%d b=%d: the site flipped back, or the fresh site did not start on a", ta.connects.Load(), tb.connects.Load())
	}
}

// A route in another timezone than Chrome's only serves as a detour: the
// site is not pinned there and returns to the first route.
func TestAffinityOtherTimeZoneIsDetourOnly(t *testing.T) {
	withAffinity(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), "America/Los_Angeles", tb))
	site := uniqueSite("tz")
	ta.refused = map[string]error{"login." + site: socksReply(5)}
	mustDial(t, r, "login."+site)
	mustDial(t, r, "www."+site)
	if tb.connects.Load() != 1 || ta.connects.Load() != 2 {
		t.Fatalf("a=%d b=%d, want one detour to b, then back on a", ta.connects.Load(), tb.connects.Load())
	}
}

// Without timezone metadata no route can be checked against Chrome's, so
// nothing is pinned: fifo, flip-back included.
func TestAffinityWithoutTimeZoneIsFIFO(t *testing.T) {
	for _, name := range []string{"no route timezones", "no chrome timezone"} {
		t.Run(name, func(t *testing.T) {
			withAffinity(t)
			ta, tb := &refusingTransport{}, &refusingTransport{}
			zone, chrome := "", chromeTZ
			if name == "no chrome timezone" {
				zone, chrome = chromeTZ, ""
			}
			r := affinityRouterTZ(t, chrome, route(uniqueID("a"), zone, ta), route(uniqueID("b"), zone, tb))
			site := uniqueSite("notz")
			ta.refused = map[string]error{"login." + site: socksReply(5)}
			mustDial(t, r, "login."+site)
			mustDial(t, r, "www."+site)
			if ta.connects.Load() != 2 || tb.connects.Load() != 1 {
				t.Fatalf("a=%d b=%d, want fifo", ta.connects.Load(), tb.connects.Load())
			}
		})
	}
}

// A refusal of the pinned route moves the site: to another allowed route
// the pin moves; back on its default route the pin is dropped.
func TestAffinityRefusalOfPinnedRouteMovesSite(t *testing.T) {
	events := withAffinity(t)
	a, b, c := uniqueID("a"), uniqueID("b"), uniqueID("c")
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb), route(c, chromeTZ, tc))
	site := uniqueSite("move")
	ta.refused = map[string]error{"login." + site: socksReply(5), "pay." + site: socksReply(5)}
	mustDial(t, r, "login."+site) // pinned to b
	tb.refused = map[string]error{"pay." + site: socksReply(5)}
	mustDial(t, r, "pay."+site) // b and a refuse -> c
	moved := events.forSite(site)
	if len(moved) != 1 || moved[0].FromRouteID != b || moved[0].RouteID != c || moved[0].Reason != pineroute.MoveSiteRefused {
		t.Fatalf("events = %+v", moved)
	}
	mustDial(t, r, "www."+site)
	if tc.connects.Load() != 2 {
		t.Fatal("the site did not stay on its new pin")
	}
	tc.refused = map[string]error{"api." + site: socksReply(2)}
	mustDial(t, r, "api."+site) // c refuses -> back on the default a
	moved = events.forSite(site)
	if len(moved) != 2 || moved[1].RouteID != a || moved[1].Reason != pineroute.MoveSiteRefused {
		t.Fatalf("events = %+v", moved)
	}
	before := ta.connects.Load()
	mustDial(t, r, "img."+site)
	if ta.connects.Load() != before+1 {
		t.Fatal("site back on its default route was not on fifo")
	}
}

// quarantineRoute charges the first route bad with escalationHosts distinct
// hosts that it fails with reply 4 while the next route reaches them.
func quarantineRoute(t *testing.T, r *xchain.Router, bad *refusingTransport) {
	t.Helper()
	if bad.refused == nil {
		bad.refused = map[string]error{}
	}
	for i := 0; i < escalationHosts; i++ {
		host := "q." + uniqueSite("eject")
		bad.refused[host] = socksReply(4)
		mustDial(t, r, host)
	}
}

// Route-wide ejection of the pinned route moves the site.
func TestAffinityEjectionMovesPinnedSite(t *testing.T) {
	events := withAffinity(t)
	a, b, c := uniqueID("a"), uniqueID("b"), uniqueID("c")
	ta, tb, tc := &refusingTransport{}, &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb), route(c, chromeTZ, tc))
	site := uniqueSite("ej")
	ta.refused = map[string]error{"login." + site: socksReply(2)}
	mustDial(t, r, "login."+site) // pinned to b
	// Charge b: hosts a refuses by policy, b fails with reply 4, c reaches.
	tb.refused = map[string]error{}
	for i := 0; i < escalationHosts; i++ {
		host := "q." + uniqueSite("ejb")
		ta.refused[host] = socksReply(2)
		tb.refused[host] = socksReply(4)
		mustDial(t, r, host)
	}
	mustDial(t, r, "www."+site)
	moved := events.forSite(site)
	if len(moved) != 1 || moved[0].FromRouteID != b || moved[0].Reason != pineroute.MoveRouteEjected {
		t.Fatalf("events = %+v", moved)
	}
}

// A refreshed plan that keeps the pinned route keeps the pin; one without
// it moves the site back to fifo.
func TestAffinityPlanRefreshKeepsOrMovesPin(t *testing.T) {
	events := withAffinity(t)
	a, b, c := uniqueID("a"), uniqueID("b"), uniqueID("c")
	ta, tb := &refusingTransport{}, &refusingTransport{}
	kept, gone := uniqueSite("kept"), uniqueSite("gone")
	ta.refused = map[string]error{"login." + kept: socksReply(2), "login." + gone: socksReply(2)}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb))
	mustDial(t, r, "login."+kept)
	mustDial(t, r, "login."+gone)
	// Same routes, new connectors (a reload): the pin stays.
	tb2 := &refusingTransport{}
	mustDial(t, affinityRouter(t, route(a, chromeTZ, &refusingTransport{}), route(b, chromeTZ, tb2)), "www."+kept)
	if tb2.connects.Load() != 1 || len(events.forSite(kept)) != 0 {
		t.Fatal("pin lost on a reload that kept the route")
	}
	// b leaves the plan.
	ta3 := &refusingTransport{}
	mustDial(t, affinityRouter(t, route(a, chromeTZ, ta3), route(c, chromeTZ, &refusingTransport{})), "www."+gone)
	moved := events.forSite(gone)
	if ta3.connects.Load() != 1 || len(moved) != 1 || moved[0].Reason != pineroute.MoveRouteRemoved || moved[0].RouteID != a {
		t.Fatalf("a=%d events=%+v", ta3.connects.Load(), moved)
	}
}

// A pinned route whose exit is now in another timezone (requalified) loses
// the pin on its next use.
func TestAffinityPinnedRouteChangingTimeZoneMoves(t *testing.T) {
	events := withAffinity(t)
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("rez")
	ta := &refusingTransport{refused: map[string]error{"login." + site: socksReply(2)}}
	mustDial(t, affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, &refusingTransport{})), "login."+site)
	ta2, tb2 := &refusingTransport{}, &refusingTransport{}
	mustDial(t, affinityRouter(t, route(a, chromeTZ, ta2), route(b, "America/Los_Angeles", tb2)), "www."+site)
	moved := events.forSite(site)
	if ta2.connects.Load() != 1 || tb2.connects.Load() != 0 || len(moved) != 1 || moved[0].Reason != pineroute.MoveRouteRemoved {
		t.Fatalf("a=%d b=%d events=%+v", ta2.connects.Load(), tb2.connects.Load(), moved)
	}
}

// A login site is pinned on its first success, even on its default route,
// so a transient refusal returns the error instead of another IP; ejection
// then moves it, reported with login_site.
func TestAffinityLoginSiteHoldsUntilEjection(t *testing.T) {
	events := withAffinity(t)
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("login")
	login := "accounts." + site
	withLoginSites(t, login)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb))
	mustDial(t, r, login)
	ta.refused = map[string]error{login: socksReply(5)}
	for i := 0; i < 2; i++ {
		if err := dial(t, r, login+":443"); err == nil || !strings.Contains(err.Error(), "reply 5") {
			t.Fatalf("attempt %d: err = %v, want the pinned route's refusal", i, err)
		}
	}
	if tb.connects.Load() != 0 {
		t.Fatal("login site moved to another IP on a transient refusal")
	}
	quarantineRoute(t, r, ta)
	mustDial(t, r, login)
	moved := events.forSite(site)
	if len(moved) != 1 || moved[0].Reason != pineroute.MoveRouteEjected || !moved[0].LoginSite ||
		moved[0].FromRouteID != a || moved[0].RouteID != b {
		t.Fatalf("events = %+v", moved)
	}
}

// A login pin that moves back onto the default route stays a pin, so the
// hold keeps protecting the site there.
func TestAffinityLoginPinMovingToDefaultKeepsHold(t *testing.T) {
	withAffinity(t)
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("loginback")
	login, pay := "accounts."+site, "pay."+site
	withLoginSites(t, login, pay)
	ta := &refusingTransport{refused: map[string]error{login: socksReply(2)}}
	tb := &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb))
	mustDial(t, r, login) // a refuses by policy -> pinned to b
	tb.refused = map[string]error{pay: socksReply(2)}
	mustDial(t, r, pay) // b refuses by policy -> back on a, still pinned
	ta.refused = map[string]error{pay: socksReply(5)}
	before := tb.connects.Load()
	if err := dial(t, r, pay+":443"); err == nil {
		t.Fatal("login site on its default route was not held")
	}
	if tb.connects.Load() != before {
		t.Fatal("held login site tried another route")
	}
}

func TestAffinityLoginSiteMovesOnPolicyRefusal(t *testing.T) {
	events := withAffinity(t)
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("policy")
	login := "accounts." + site
	withLoginSites(t, login)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb))
	mustDial(t, r, login)
	ta.refused = map[string]error{login: socksReply(2)}
	mustDial(t, r, login)
	moved := events.forSite(site)
	if len(moved) != 1 || moved[0].Reason != pineroute.MoveSiteRefused || !moved[0].LoginSite || moved[0].RouteID != b {
		t.Fatalf("events = %+v", moved)
	}
}

// ejectSecond makes route b fail route-wide once, which ejects it and uses
// up the two-route hop's ejection cap, so a later failure of route a only
// cools it down.
func ejectSecond(t *testing.T, r *xchain.Router, ta, tb *refusingTransport) {
	t.Helper()
	host := "x." + uniqueSite("second")
	ta.refused = map[string]error{host: socksReply(2)}
	tb.handshakeErr = socksReply(1)
	_ = dial(t, r, host+":443")
	tb.handshakeErr = nil
	ta.refused = nil
}

// Once a login site's route has failed for the hold window, the site moves
// (route_failed) rather than staying on a dead route.
func TestAffinityLoginHoldExpiresAfterRouteFailures(t *testing.T) {
	events := withAffinity(t)
	previous := pineroute.SetLoginHold(time.Millisecond)
	t.Cleanup(func() { pineroute.SetLoginHold(previous) })
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("expire")
	login := "accounts." + site
	withLoginSites(t, login)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb))
	mustDial(t, r, login)
	ejectSecond(t, r, ta, tb)
	before := tb.connects.Load()
	ta.handshakeErr = socksReply(1)
	for i := 0; i < 3; i++ {
		if err := dial(t, r, login+":443"); err == nil {
			t.Fatalf("attempt %d moved before the hold expired", i)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if tb.connects.Load() != before {
		t.Fatal("held login site tried another route")
	}
	mustDial(t, r, login)
	moved := events.forSite(site)
	if len(moved) != 1 || moved[0].Reason != pineroute.MoveRouteFailed || !moved[0].LoginSite || moved[0].RouteID != b {
		t.Fatalf("events = %+v", moved)
	}
}

// Login hosts default to the ISP route (whitelist), other hosts of the
// same site to the primary; neither moves the other.
func TestAffinityISPWhitelistSeparatesLoginHosts(t *testing.T) {
	events := withAffinity(t)
	site := uniqueSite("isp")
	login := "accounts." + site
	withLoginSites(t, login)
	tisp, tp := &refusingTransport{}, &refusingTransport{}
	ispNode := route(uniqueID("isp"), chromeTZ, tisp, corechain.BypassNodeOption(whitelist{loginBypass{hosts: map[string]bool{login: true}}}))
	r := affinityRouter(t, ispNode, route(uniqueID("p"), chromeTZ, tp))
	for i := 0; i < 3; i++ {
		mustDial(t, r, login)
		mustDial(t, r, "www."+site)
	}
	if tisp.connects.Load() != 3 || tp.connects.Load() != 3 || len(events.forSite(site)) != 0 {
		t.Fatalf("isp=%d primary=%d events=%+v", tisp.connects.Load(), tp.connects.Load(), events.forSite(site))
	}
}

// A direct winner drops the pin without an event; the site is back on fifo.
func TestAffinityDirectWinnerDropsPinSilently(t *testing.T) {
	events := withAffinity(t)
	site := uniqueSite("direct")
	ta := &refusingTransport{refused: map[string]error{"login." + site: socksReply(2), "pay." + site: socksReply(2)}}
	tb := &refusingTransport{}
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb), directBackupNode())
	mustDial(t, r, "login."+site) // pinned to b
	tb.refused = map[string]error{"pay." + site: socksReply(2)}
	mustDial(t, r, "pay."+site) // a and b refuse -> direct
	if len(events.forSite(site)) != 0 {
		t.Fatalf("direct winner emitted %+v", events.forSite(site))
	}
	before := ta.connects.Load()
	mustDial(t, r, "www."+site)
	if ta.connects.Load() != before+1 {
		t.Fatal("pin survived a move to direct")
	}
}

// Concurrent requests that all see the pinned route refuse the site produce
// one move.
func TestAffinityConcurrentMoveIsSingle(t *testing.T) {
	events := withAffinity(t)
	site := uniqueSite("conc")
	ta := &refusingTransport{refused: map[string]error{"login." + site: socksReply(2), "www." + site: socksReply(2)}}
	tb, tc := &refusingTransport{}, &refusingTransport{}
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb), route(uniqueID("c"), chromeTZ, tc))
	mustDial(t, r, "login."+site)
	tb.refused = map[string]error{"www." + site: socksReply(5)}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = dial(t, r, "www."+site+":443")
		}()
	}
	wg.Wait()
	if moved := events.forSite(site); len(moved) != 1 {
		t.Fatalf("%d moves: %+v", len(moved), moved)
	}
}

func directBackupNode() *corechain.Node {
	return corechain.NewNode("direct", "", corechain.TransportNodeOption(&refusingTransport{}),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct", "pine_tier": "direct", "backup": true})))
}

// A pin outside Chrome's timezone that still serves as the last usable route
// is dropped, not kept: the next request is back on fifo order.
func TestAffinityRemovedPinServingAsLastRouteIsDropped(t *testing.T) {
	events := withAffinity(t)
	a, b := uniqueID("a"), uniqueID("b")
	site := uniqueSite("last")
	ta := &refusingTransport{refused: map[string]error{"login." + site: socksReply(2)}}
	tb := &refusingTransport{}
	mustDial(t, affinityRouter(t, route(a, chromeTZ, ta), route(b, chromeTZ, tb)), "login."+site) // pinned to b
	// b's exit moved to another timezone and a refuses the next host: b
	// serves it as the last route.
	ta2 := &refusingTransport{refused: map[string]error{"www." + site: socksReply(2)}}
	tb2 := &refusingTransport{}
	r := affinityRouter(t, route(a, chromeTZ, ta2), route(b, "America/Los_Angeles", tb2))
	mustDial(t, r, "www."+site)
	if tb2.connects.Load() != 1 || len(events.forSite(site)) != 0 {
		t.Fatalf("b=%d events=%+v", tb2.connects.Load(), events.forSite(site))
	}
	// The pin is gone: a host a accepts goes to a.
	mustDial(t, r, "img."+site)
	if ta2.connects.Load() != 2 {
		t.Fatalf("a=%d: the dropped pin still steered the site", ta2.connects.Load())
	}
}

type hostMatcher string

func (m hostMatcher) Match(req *routing.Request) bool { return strings.HasPrefix(req.Host, string(m)) }

// A matcher node (e.g. added by a loader after parsing) would override pins
// with its priority, so a hop that has one stays plain fifo.
func TestAffinityOffWithMatcherNodes(t *testing.T) {
	withAffinity(t)
	ta, tb := &refusingTransport{}, &refusingTransport{}
	site := uniqueSite("matcher")
	ta.refused = map[string]error{"login." + site: socksReply(5)}
	matched := route(uniqueID("m"), chromeTZ, &refusingTransport{}, corechain.MatcherNodeOption(hostMatcher("never.")))
	r := affinityRouter(t, route(uniqueID("a"), chromeTZ, ta), route(uniqueID("b"), chromeTZ, tb), matched)
	mustDial(t, r, "login."+site)
	mustDial(t, r, "www."+site)
	if ta.connects.Load() != 2 || tb.connects.Load() != 1 {
		t.Fatalf("a=%d b=%d, want fifo (no pin)", ta.connects.Load(), tb.connects.Load())
	}
}
