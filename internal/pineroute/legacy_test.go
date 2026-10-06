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
	Escalation = false
	t.Cleanup(func() { Escalation = true })
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

func TestWithoutEscalationEjectionIsFixedAndUncapped(t *testing.T) {
	withoutEscalation(t)
	store, clock := newTestEjectionStore(t, 16)
	hop := testHop(2)
	for round := 0; round < 3; round++ {
		for _, route := range hop {
			if !store.eject(route, routeInfo{}, hop, ReasonDestinationFailures) {
				t.Fatal("ejection refused without escalation")
			}
		}
		for _, event := range clock.events[len(clock.events)-2:] {
			if event.K != 1 || event.TTL != defaultEjectionBase {
				t.Fatalf("round %d: %+v, want k=1 ttl=30s", round, event)
			}
		}
		clock.now = clock.now.Add(defaultEjectionBase)
		clock.fire()
		clock.now = clock.now.Add(time.Second)
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
	if PanicSelect(nodes) != nil {
		t.Fatal("panic selection ran without escalation")
	}
	for i := 0; i < 3; i++ {
		if !MergeNoRoute("tcp", "x.example:443", nil, func(any, int) {}) {
			t.Fatal("no_route failures were merged without escalation")
		}
	}
}
