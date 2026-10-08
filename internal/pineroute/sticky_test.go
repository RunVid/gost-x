package pineroute

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
	mdx "github.com/go-gost/x/metadata"
)

// uniqueHop names a hop for one test run: sticky state outlives configs by
// hop name, so repeated runs (-count) must not share it.
func uniqueHop(t *testing.T) string {
	return fmt.Sprintf("%s/%d", t.Name(), time.Now().UnixNano())
}

func zoneNode(id, zone string) *chain.Node {
	return chain.NewNode(id, "proxy.example:1080", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_id": id, "pine_route_tz": zone})))
}

// stickyRequest selects with sticky for one request and returns its context.
func stickyRequest(t *testing.T, sticky *Sticky, all ...*chain.Node) (context.Context, *StickySelection) {
	t.Helper()
	ctx := WithAttempts(context.Background())
	s := StickyFor(ctx, "tcp", sticky)
	s.Order(all, all, func(*chain.Node) bool { return false })
	return ctx, s
}

func TestStickyFirstRouteAndReloadGeneration(t *testing.T) {
	hop := uniqueHop(t)
	a, b := zoneNode("er_st_a", "America/New_York"), zoneNode("er_st_b", "America/New_York")
	sticky := NewSticky(hop, "America/New_York")
	ctx, _ := stickyRequest(t, sticky, a, b)
	if StickySuccess(ctx, a) != nil || sticky.Current() != "er_st_a" {
		t.Fatalf("first route: current %q", sticky.Current())
	}
	// Before any success a detour (no reason) through b leaves a current.
	fresh := NewSticky(hop+"/fresh", "America/New_York")
	ctx, _ = stickyRequest(t, fresh, a, b)
	if StickySuccess(ctx, b) != nil || fresh.Current() != "" {
		t.Fatalf("detour before the first success became current: %q", fresh.Current())
	}
	// A request that saw a with a reason to leave it; a reload lands before
	// it connects on b: it must not promote b.
	ctx, s := stickyRequest(t, sticky, a, b)
	s.note(MoveRouteFailed)
	reloaded := NewSticky(hop, "America/New_York") // a config load
	if move := StickySuccess(ctx, b); move != nil || reloaded.Current() != "er_st_a" {
		t.Fatalf("stale request promoted: %+v, current %q", move, reloaded.Current())
	}
	ctx, s = stickyRequest(t, reloaded, a, b)
	s.note(MoveRouteFailed)
	if move := StickySuccess(ctx, b); move == nil || move.FromRoute != "er_st_a" || move.ToRoute != "er_st_b" || move.Reason != MoveRouteFailed {
		t.Fatalf("move = %+v", move)
	}
}

func TestStickyNeverPromotesDirectOtherZoneOrLastResort(t *testing.T) {
	sticky := NewSticky(uniqueHop(t), "America/New_York")
	a := zoneNode("er_st_la", "America/New_York")
	other := zoneNode("er_st_lb", "America/Los_Angeles")
	direct := chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	next := zoneNode("er_st_lc", "America/New_York")
	ctx, _ := stickyRequest(t, sticky, a, other, direct, next)
	StickySuccess(ctx, a)
	for name, winner := range map[string]*chain.Node{"other timezone": other, "direct": direct, "last resort": next} {
		ctx, s := stickyRequest(t, sticky, a, other, direct, next)
		s.note(MoveRouteFailed)
		if name == "last resort" {
			s.MarkLastResort(next)
		}
		if move := StickySuccess(ctx, winner); move != nil || sticky.Current() != "er_st_la" {
			t.Fatalf("%s became current: %+v", name, move)
		}
	}
	// Without a reason a same-timezone winner is a detour.
	ctx, _ = stickyRequest(t, sticky, a, other, direct, next)
	if move := StickySuccess(ctx, next); move != nil || sticky.Current() != "er_st_la" {
		t.Fatalf("detour became current: %+v", move)
	}
}

func TestStickyOrder(t *testing.T) {
	sticky := NewSticky(uniqueHop(t), "America/New_York")
	a, b := zoneNode("er_so_a", "America/New_York"), zoneNode("er_so_b", "America/Chicago")
	c, d := zoneNode("er_so_c", "America/New_York"), zoneNode("er_so_d", "America/New_York")
	direct := chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	all := []*chain.Node{a, b, c, direct, d}
	ctx, _ := stickyRequest(t, sticky, all...)
	StickySuccess(ctx, a)
	ctx, s := stickyRequest(t, sticky, all...)
	s.note(MoveRouteFailed)
	StickySuccess(ctx, c)
	_, s = stickyRequest(t, sticky, all...)
	var got []string
	for _, node := range s.Order(all, all, func(*chain.Node) bool { return false }) {
		got = append(got, node.Name)
	}
	// Chrome's timezone rotated from c, then the other timezone, then direct.
	if want := "[er_so_c er_so_d er_so_a er_so_b direct]"; fmtNames(got) != want {
		t.Fatalf("order = %s, want %s", fmtNames(got), want)
	}
}

func fmtNames(names []string) string {
	out := "["
	for i, n := range names {
		if i > 0 {
			out += " "
		}
		out += n
	}
	return out + "]"
}
