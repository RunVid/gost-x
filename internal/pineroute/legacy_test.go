package pineroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
)

func withoutEscalation(t *testing.T) {
	t.Helper()
	t.Cleanup(SetEscalationForTest(false))
}

// Without PINE_GOST_ROUTE_EJECTION=on, GOST keeps the #7/#8 rules.
func TestWithoutEscalationRefusalsKeepFixedTTLs(t *testing.T) {
	withoutEscalation(t)
	cache := newRefusalCache(8)
	route := routeKey{managedID: "route"}
	key := endpointKey{route: route, network: "tcp", address: "x.example:443"}
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 4; i++ {
		cache.add(route, "tcp", "x.example:443", classPolicy, now)
		if entry := cache.entries[key]; entry.ttl != refusalTTL || !entry.expires.Equal(now.Add(refusalTTL)) {
			t.Fatalf("refusal %d: %+v, want a fixed 10 min", i+1, entry)
		}
		now = now.Add(refusalTTL)
	}
	node := pineNode("er_legacy_success")
	ctx := WithAttempts(context.Background())
	RecordRefusal(ctx, node, "tcp", "kept.example:443", replyError(2))
	RecordSuccess(ctx, node, "tcp", "kept.example:443")
	if !Skip(WithAttempts(context.Background()), node, "tcp", "kept.example:443") {
		t.Fatal("a success cleared a refusal without escalation (#7 never did)")
	}
}

func TestWithoutEscalationEjectionIsFixedUncappedAndRenewed(t *testing.T) {
	withoutEscalation(t)
	store, clock := newTestEjectionStore(t, 16)
	store.emit = emitRouteEvent
	var events []RouteEvent
	SetRouteEventSink(func(e RouteEvent) { events = append(events, e) })
	defer SetRouteEventSink(nil)
	hop := testHop(2)
	for round := 0; round < 3; round++ {
		for _, route := range hop {
			if !store.eject(route, routeInfo{}, hop, ReasonDestinationFailures) {
				t.Fatal("ejection refused without escalation")
			}
			if record := store.entries[route]; record.k != 1 || !record.until.Equal(clock.now.Add(defaultEjectionBase)) {
				t.Fatalf("round %d: %+v, want a fixed 30 s", round, *record)
			}
		}
		// Fresh evidence 20 s in renews the quarantine (#8).
		clock.now = clock.now.Add(20 * time.Second)
		store.eject(hop[0], routeInfo{}, hop, ReasonDestinationFailures)
		clock.now = clock.now.Add(15 * time.Second)
		clock.fire()
		if !store.ejected(hop[0], clock.now) || store.ejected(hop[1], clock.now) {
			t.Fatalf("round %d: renewal not honoured", round)
		}
		clock.now = clock.now.Add(defaultEjectionBase)
		clock.fire()
		clock.now = clock.now.Add(time.Second)
	}
	if len(events) != 0 {
		t.Fatalf("%d route events without escalation, want none", len(events))
	}
}

func TestWithoutEscalationExpiredRefusalsAreDroppedFirst(t *testing.T) {
	withoutEscalation(t)
	cache := newRefusalCache(2)
	route := routeKey{managedID: "route"}
	now := time.Unix(1_700_000_000, 0)
	cache.add(route, "tcp", "old.example:443", classPolicy, now)
	cache.add(route, "tcp", "live.example:443", classTransient, now.Add(refusalTTL))
	// old.example expired; a new refusal must evict it, not the live one.
	cache.add(route, "tcp", "new.example:443", classTransient, now.Add(refusalTTL+time.Second))
	if !cache.contains(route, "tcp", "live.example:443", now.Add(refusalTTL+time.Second)) {
		t.Fatal("a live refusal was evicted ahead of an expired one")
	}
	if cache.contains(route, "tcp", "old.example:443", now.Add(refusalTTL)) {
		t.Fatal("an expired refusal still excluded its pair")
	}
}

func TestWithoutEscalationNoRouteWideEjectionPanicOrMerge(t *testing.T) {
	withoutEscalation(t)
	defer SetTimingForTest(defaultEjectionBase, time.Second)()
	nodes := []*chain.Node{pineNode("er_legacy_a"), pineNode("er_legacy_b")}
	ctx := WithAttempts(context.Background())
	NoteHop(ctx, nodes)
	RecordRouteFailure(ctx, nodes[0], "tcp", errors.New("proxy reset"))
	if Ejected(nodes[0]) {
		t.Fatal("a route-wide failure ejected a route without escalation")
	}
	if PanicSelect(ctx, nodes) != nil {
		t.Fatal("panic selection ran without escalation")
	}
	for i := 0; i < 3; i++ {
		if !MergeNoRoute("tcp", "x.example:443", nil, func(any, int) {}) {
			t.Fatal("no_route failures were merged without escalation")
		}
	}
}
