package pineroute

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/gosocks5"
	mdx "github.com/go-gost/x/metadata"
)

type fakeTimer struct {
	due time.Time
	f   func()
}

type fakeEjectionClock struct {
	now    time.Time
	timers []fakeTimer
	events []RouteEvent
}

func newTestEjectionStore(t *testing.T, limit int) (*ejectionStore, *fakeEjectionClock) {
	t.Helper()
	clock := &fakeEjectionClock{now: time.Unix(1_700_000_000, 0)}
	store := newEjectionStore(limit, defaultEjectionBase)
	store.now = func() time.Time { return clock.now }
	store.after = func(d time.Duration, f func()) { clock.timers = append(clock.timers, fakeTimer{clock.now.Add(d), f}) }
	store.jitter = func(time.Duration) time.Duration { return 0 }
	store.emit = func(event RouteEvent) { clock.events = append(clock.events, event) }
	return store, clock
}

// fire runs the restore timers that are due.
func (c *fakeEjectionClock) fire() {
	var pending []fakeTimer
	var due []func()
	for _, timer := range c.timers {
		if timer.due.After(c.now) {
			pending = append(pending, timer)
		} else {
			due = append(due, timer.f)
		}
	}
	c.timers = pending
	for _, f := range due {
		f()
	}
}

func testHop(n int) []incarnation {
	hop := make([]incarnation, n)
	for i := range hop {
		id := fmt.Sprintf("er_hop_%d", i)
		hop[i] = incarnation{id: id, marker: pineNode(id).Marker()}
	}
	return hop
}

func TestEjectionEscalatesUpToTheCap(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(8)
	route := hop[0]
	for want := 1; want <= maxEjectionK+2; want++ {
		if !store.eject(route, routeInfo{tier: "primary"}, hop, ReasonTimeout) {
			t.Fatalf("ejection %d refused", want)
		}
		k := min(want, maxEjectionK)
		ttl := min(time.Duration(k)*defaultEjectionBase, 10*time.Minute)
		last := clock.events[len(clock.events)-1]
		if last.Kind != RouteEjected || last.K != k || last.TTL != ttl || last.Reason != ReasonTimeout || last.Tier != "primary" {
			t.Fatalf("ejection %d: event %+v, want k=%d ttl=%s", want, last, k, ttl)
		}
		clock.now = clock.now.Add(ttl)
		clock.fire()
		if restored := clock.events[len(clock.events)-1]; restored.Kind != RouteRestored || restored.K != k || restored.TTL != ttl {
			t.Fatalf("ejection %d: restore event %+v", want, restored)
		}
		// Fail again right after the restore: no healthy window has passed.
		clock.now = clock.now.Add(time.Second)
	}
	if store.entries[route].k != maxEjectionK {
		t.Fatalf("k=%d, want capped at %d", store.entries[route].k, maxEjectionK)
	}
}

func TestEjectionDecaysPerHealthyWindow(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(4)
	route := hop[0]
	for i := 0; i < 3; i++ {
		store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
		clock.now = clock.now.Add(store.entries[route].ttl)
		clock.fire()
	}
	if k := store.entries[route].k; k != 3 {
		t.Fatalf("k=%d after three ejections", k)
	}
	// Two full healthy windows since the restore: k 3 → 1, then +1.
	clock.now = clock.now.Add(2*healthyWindowFactor*defaultEjectionBase + time.Second)
	store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
	if got := clock.events[len(clock.events)-1]; got.K != 2 || got.TTL != 2*defaultEjectionBase {
		t.Fatalf("after decay: %+v, want k=2", got)
	}
	clock.now = clock.now.Add(store.entries[route].ttl)
	clock.fire()
	// A long healthy period resets k completely.
	clock.now = clock.now.Add(time.Hour)
	store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
	if got := clock.events[len(clock.events)-1]; got.K != 1 || got.TTL != defaultEjectionBase {
		t.Fatalf("after a healthy hour: %+v, want k=1", got)
	}
}

func TestEvidenceBelowThresholdBlocksDecay(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(4)
	route := hop[0]
	for i := 0; i < 2; i++ {
		store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
		clock.now = clock.now.Add(store.entries[route].ttl)
		clock.fire()
	}
	// Charges keep arriving every 50 s for five minutes: no window is healthy.
	for i := 0; i < 6; i++ {
		clock.now = clock.now.Add(50 * time.Second)
		store.noteBad(route)
	}
	clock.now = clock.now.Add(50 * time.Second)
	store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
	if got := clock.events[len(clock.events)-1]; got.K != 3 {
		t.Fatalf("k=%d, want 3: a route still failing must not decay", got.K)
	}
}

func TestEjectedRouteIsNotEjectedAgain(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(4)
	if !store.eject(hop[0], routeInfo{}, hop, ReasonConnectError) {
		t.Fatal("first ejection refused")
	}
	clock.now = clock.now.Add(time.Second)
	if store.eject(hop[0], routeInfo{}, hop, ReasonConnectError) {
		t.Fatal("an ejected route was ejected again")
	}
	if len(clock.events) != 1 || len(clock.timers) != 1 {
		t.Fatalf("events=%d timers=%d, want one each", len(clock.events), len(clock.timers))
	}
	if !store.ejected(hop[0], clock.now) || store.ejected(hop[0], clock.now.Add(defaultEjectionBase)) {
		t.Fatal("ejection period is wrong")
	}
}

func TestEjectionCapIsHalfTheHop(t *testing.T) {
	for _, m := range []int{1, 2, 3, 4, 8, 9} {
		t.Run(fmt.Sprint(m), func(t *testing.T) {
			store, clock := newTestEjectionStore(t, 32)
			hop := testHop(m)
			ejected := 0
			for _, route := range hop {
				if store.eject(route, routeInfo{}, hop, ReasonTimeout) {
					ejected++
				}
			}
			if want := max(1, m/2); ejected != want || len(clock.events) != want {
				t.Fatalf("ejected %d of %d (events %d), want %d", ejected, m, len(clock.events), want)
			}
			// Once one restores, another may go.
			if m >= 2 {
				clock.now = clock.now.Add(defaultEjectionBase)
				clock.fire()
				if !store.eject(hop[m-1], routeInfo{}, hop, ReasonTimeout) {
					t.Fatal("cap did not free up after restores")
				}
			}
		})
	}
}

func TestCappedEjectionCountsAsEvidenceButKeepsEarnedDecay(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(2)
	for i := 0; i < 3; i++ {
		store.eject(hop[1], routeInfo{}, hop, ReasonTimeout)
		clock.now = clock.now.Add(store.entries[hop[1]].ttl)
		clock.fire()
	}
	clock.now = clock.now.Add(3 * time.Hour)
	store.eject(hop[0], routeInfo{}, hop, ReasonTimeout)
	if store.eject(hop[1], routeInfo{}, hop, ReasonTimeout) {
		t.Fatal("cap exceeded")
	}
	if !store.entries[hop[1]].lastBad.Equal(clock.now) {
		t.Fatalf("capped route: %+v", *store.entries[hop[1]])
	}
	clock.now = clock.now.Add(defaultEjectionBase)
	clock.fire()
	store.eject(hop[1], routeInfo{}, hop, ReasonTimeout)
	if got := clock.events[len(clock.events)-1]; got.Kind != RouteEjected || got.K != 1 {
		t.Fatalf("%+v: capped evidence after three healthy hours must not keep the old k", got)
	}
}

func TestConcurrentEjectionsNeverExceedTheCap(t *testing.T) {
	store := newEjectionStore(64, defaultEjectionBase)
	store.after = func(time.Duration, func()) {}
	store.emit = func(RouteEvent) {}
	hop := testHop(9)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ejected := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if store.eject(hop[i%len(hop)], routeInfo{}, hop, ReasonTimeout) {
				mu.Lock()
				ejected++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if ejected != 4 {
		t.Fatalf("ejected %d routes of 9, want 4", ejected)
	}
}

func TestFullStoreKeepsActiveEjections(t *testing.T) {
	store, clock := newTestEjectionStore(t, 2)
	hop := testHop(8)
	store.eject(hop[0], routeInfo{}, hop, ReasonTimeout)
	store.noteBad(hop[1])
	// The non-ejected record makes room; the active ejection stays.
	if !store.eject(hop[2], routeInfo{}, hop, ReasonTimeout) {
		t.Fatal("a full store did not evict a non-ejected record")
	}
	if !store.ejected(hop[0], clock.now) || !store.ejected(hop[2], clock.now) {
		t.Fatal("an active ejection was dropped")
	}
	// Full of active ejections: new evidence is not recorded.
	if store.eject(hop[3], routeInfo{}, hop, ReasonTimeout) {
		t.Fatal("a store full of active ejections took another route")
	}
	store.noteBad(hop[3])
	if len(store.entries) != 2 {
		t.Fatalf("entries=%d, want 2", len(store.entries))
	}
	clock.now = clock.now.Add(defaultEjectionBase)
	clock.fire()
	restored := 0
	for _, event := range clock.events {
		if event.Kind == RouteRestored {
			restored++
		}
	}
	if restored != 2 {
		t.Fatalf("restored %d ejections, want 2", restored)
	}
}

func TestEjectionFollowsIncarnation(t *testing.T) {
	restore := SetTimingForTest(time.Hour/120, time.Second)
	defer restore()
	loaded := pineNode("er_reload")
	other := pineNode("er_reload_other")
	ctx := WithAttempts(context.Background())
	NoteHop(ctx, append([]*chain.Node{loaded, other}, pineNode("er_reload_3"), pineNode("er_reload_4")))
	RecordRouteFailure(ctx, loaded, "tcp", errors.New("proxy reset"))
	if !Ejected(loaded) || !Ejected(loaded.Copy()) {
		t.Fatal("route or its copy is not ejected")
	}
	if Ejected(pineNode("er_reload")) {
		t.Fatal("a reloaded route inherited its predecessor's ejection")
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestRouteFailureReasons(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"auth":     {fmt.Errorf("handshake: %w", gosocks5.ErrAuthFailure), ReasonAuth},
		"method":   {gosocks5.ErrBadMethod, ReasonAuth},
		"deadline": {fmt.Errorf("dial: %w", context.DeadlineExceeded), ReasonTimeout},
		"net":      {&net.OpError{Op: "dial", Err: timeoutError{}}, ReasonTimeout},
		"general":  {replyError(1), ReasonProxyFailure},
		"command":  {replyError(7), ReasonProxyFailure},
		"reset":    {errors.New("connection reset by peer"), ReasonConnectError},
	} {
		if got := routeFailureReason(tc.err); got != tc.want {
			t.Errorf("%s: reason %q, want %q", name, got, tc.want)
		}
	}
}

func TestOnlyRouteWideFailuresEject(t *testing.T) {
	restore := SetTimingForTest(defaultEjectionBase, time.Second)
	defer restore()
	nodes := []*chain.Node{pineNode("er_wide_a"), pineNode("er_wide_b"), pineNode("er_wide_c"), pineNode("er_wide_d")}
	direct := chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	ctx := WithAttempts(context.Background())
	NoteHop(ctx, append(nodes, direct))
	for _, err := range []error{replyError(2), replyError(4), replyError(5), replyError(8), context.Canceled} {
		RecordRouteFailure(ctx, nodes[0], "tcp", err)
	}
	RecordRouteFailure(ctx, direct, "tcp", errors.New("refused"))
	if Ejected(nodes[0]) || Ejected(direct) {
		t.Fatal("a destination failure, cancellation or direct failure ejected a route")
	}
	canceled, cancel := context.WithCancel(context.Background())
	tracked := WithAttempts(canceled)
	NoteHop(tracked, nodes)
	cancel()
	RecordRouteFailure(tracked, nodes[1], "tcp", errors.New("proxy reset"))
	if Ejected(nodes[1]) {
		t.Fatal("a canceled request ejected a route")
	}
	RecordRouteFailure(ctx, nodes[2], "tcp", replyError(1))
	if !Ejected(nodes[2]) {
		t.Fatal("a general failure did not eject the route")
	}
}

func TestPanicSelectOrder(t *testing.T) {
	restore := SetTimingForTest(defaultEjectionBase, time.Second)
	defer restore()
	primary := pineNode("er_panic_primary")
	ejectedPrimary := pineNode("er_panic_ejected")
	backup := chain.NewNode("er_panic_backup", "b:1", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_id": "er_panic_backup", "backup": true})))
	ctx := WithAttempts(context.Background())
	NoteHop(ctx, []*chain.Node{ejectedPrimary, primary, backup, pineNode("er_panic_4")})
	RecordRouteFailure(ctx, ejectedPrimary, "tcp", errors.New("reset"))
	direct := chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct", "backup": true})))
	request := func() context.Context { return WithAttempts(context.Background()) }
	if got := PanicSelect(request(), []*chain.Node{direct, ejectedPrimary, backup, primary}); got != primary {
		t.Fatalf("panic picked %v, want the non-ejected primary", got.Name)
	}
	if got := PanicSelect(request(), []*chain.Node{direct, backup, ejectedPrimary}); got != ejectedPrimary {
		t.Fatalf("panic picked %v, want the ejected primary before any backup", got.Name)
	}
	if got := PanicSelect(request(), []*chain.Node{direct, backup}); got != backup {
		t.Fatalf("panic picked %v, want the managed backup before direct", got.Name)
	}
	if got := PanicSelect(request(), []*chain.Node{direct}); got != direct {
		t.Fatal("panic did not use direct as the last resort of an automatic plan")
	}
	if PanicSelect(request(), nil) != nil {
		t.Fatal("panic invented a route")
	}
	once := request()
	if PanicSelect(once, []*chain.Node{ejectedPrimary}) != ejectedPrimary || PanicSelect(once, []*chain.Node{primary}) != nil {
		t.Fatal("a request gets exactly one panic attempt")
	}
	Enabled = false
	defer func() { Enabled = true }()
	if PanicSelect(request(), []*chain.Node{primary}) != nil {
		t.Fatal("panic selection ran outside Pine")
	}
}

// Evidence below the ejection threshold must not erase healthy time already
// earned: an old ejection decays before new charges move the clock.
func TestChargesAfterALongHealthyPeriodDoNotKeepOldK(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(4)
	route := hop[0]
	for i := 0; i < 3; i++ {
		store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
		clock.now = clock.now.Add(store.entries[route].ttl)
		clock.fire()
	}
	clock.now = clock.now.Add(3 * time.Hour)
	store.noteBad(route)
	clock.now = clock.now.Add(10 * time.Second)
	store.noteBad(route)
	clock.now = clock.now.Add(10 * time.Second)
	store.eject(route, routeInfo{}, hop, ReasonDestinationFailures)
	if got := clock.events[len(clock.events)-1]; got.K != 1 {
		t.Fatalf("k=%d after three healthy hours, want 1", got.K)
	}
}

// A restore timer that runs late must not lose the route_restored of an
// ejection that a new ejection replaces first.
func TestLateRestoreTimerStillReportsTheRestore(t *testing.T) {
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(4)
	store.eject(hop[0], routeInfo{}, hop, ReasonTimeout)
	// The TTL passes but the timer has not run yet.
	clock.now = clock.now.Add(defaultEjectionBase + time.Second)
	if !store.eject(hop[0], routeInfo{}, hop, ReasonTimeout) {
		t.Fatal("an expired ejection blocked the next one")
	}
	kinds := []string{}
	for _, event := range clock.events {
		kinds = append(kinds, event.Kind)
	}
	if fmt.Sprint(kinds) != fmt.Sprint([]string{RouteEjected, RouteRestored, RouteEjected}) {
		t.Fatalf("events %v, want ejected, restored, ejected", kinds)
	}
	clock.now = clock.now.Add(time.Hour)
	clock.fire()
	if n := len(clock.events); n != 4 || clock.events[3].Kind != RouteRestored {
		t.Fatalf("after all timers: %d events, last %+v", n, clock.events[n-1])
	}
}
