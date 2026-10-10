package pineroute

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-gost/core/chain"
)

func TestExclusionCauseRules(t *testing.T) {
	a := routeKey{managedID: "er_a"}
	b := routeKey{managedID: "er_b"}
	c := routeKey{managedID: "er_c"}
	plan := func(routes ...routeKey) map[routeKey]struct{} {
		m := map[routeKey]struct{}{}
		for _, route := range routes {
			m[route] = struct{}{}
		}
		return m
	}
	reply := func(route routeKey, code uint8) attemptRecord {
		return attemptRecord{route: route, dialed: "tcp/site:443", failed: true, reply: code, destination: true}
	}
	transport := func(route routeKey) attemptRecord {
		return attemptRecord{route: route, dialed: "tcp/site:443", failed: true}
	}
	type excl = map[routeKey]exclusion
	cases := map[string]struct {
		records  []attemptRecord
		plan     map[routeKey]struct{}
		excluded excl
		want     string
	}{
		"no plan":                     {nil, nil, nil, CauseUnknown},
		"nothing excluded or tried":   {nil, plan(a), nil, CauseUnknown},
		"policy exclusions only":      {nil, plan(a, b), excl{a: excludedPolicy, b: excludedPolicy}, CauseVendorPolicy},
		"transient and policy":        {nil, plan(a, b), excl{a: excludedTransient, b: excludedPolicy}, CauseSite},
		"transient exclusions only":   {nil, plan(a, b), excl{a: excludedTransient, b: excludedTransient}, CauseSite},
		"policy reply and exclusion":  {[]attemptRecord{reply(a, 2)}, plan(a, b), excl{b: excludedPolicy}, CauseVendorPolicy},
		"refused on every route":      {[]attemptRecord{reply(a, 4)}, plan(a, b, c), excl{b: excludedTransient, c: excludedPolicy}, CauseSite},
		"single route refused":        {[]attemptRecord{reply(a, 4)}, plan(a), nil, CauseSite},
		"one node not covered":        {[]attemptRecord{reply(a, 4)}, plan(a, b), nil, CauseUnknown},
		"cooldown only":               {nil, plan(a, b), excl{a: excludedCooldown, b: excludedCooldown}, CauseNetwork},
		"ejected only":                {nil, plan(a, b), excl{a: excludedEjected, b: excludedEjected}, CauseNetwork},
		"route failure and cooldown":  {[]attemptRecord{transport(a)}, plan(a, b), excl{b: excludedCooldown}, CauseNetwork},
		"route failure and ejection":  {[]attemptRecord{transport(a)}, plan(a, b, c), excl{b: excludedEjected}, CauseNetwork},
		"single route failure":        {[]attemptRecord{transport(a)}, plan(a), nil, CauseNetwork},
		"refusal beats cooldown":      {nil, plan(a, b), excl{a: excludedTransient, b: excludedCooldown}, CauseUnknown},
		"destination beats cooldown":  {[]attemptRecord{reply(a, 4)}, plan(a, b), excl{b: excludedCooldown}, CauseUnknown},
		"destination and route":       {[]attemptRecord{reply(a, 4), transport(b)}, plan(a, b), nil, CauseUnknown},
		"address family only":         {[]attemptRecord{reply(a, 8), reply(b, 8)}, plan(a, b), nil, CauseUnknown},
		"address family and cooldown": {[]attemptRecord{reply(a, 8)}, plan(a, b), excl{b: excludedCooldown}, CauseNetwork},
	}
	for name, tc := range cases {
		if got := exclusionCause(tc.records, tc.plan, tc.excluded); got != tc.want {
			t.Errorf("%s: cause = %q, want %q", name, got, tc.want)
		}
	}
}

func TestExclusionsCountUntriedNodesOnceWithLatestReason(t *testing.T) {
	ctx := WithAttempts(context.Background())
	if got := Exclusions(ctx); got != nil {
		t.Fatalf("no exclusion: %+v, want nil", got)
	}
	a, b, c, d := pineNode("er_x_a"), pineNode("er_x_b"), pineNode("er_x_c"), pineNode("er_x_d")
	NoteHop(ctx, []*chain.Node{a, b, c, d})
	NoteCooldown(ctx, a, a.Copy(), b)
	NoteEjected(ctx, b) // latest reason wins
	NoteCooldown(ctx, c)
	MarkTried(ctx, c) // attempted later: not excluded
	host := fmt.Sprintf("excluded-%d.example:443", time.Now().UnixNano())
	defaultRefusals.add(nodeRouteKey(d), "tcp", host, classPolicy, time.Now())
	NoteRefusal(ctx, d, "tcp", host)
	NoteRefusal(ctx, a, "tcp", host) // not refused: keeps cooldown
	want := Excluded{Policy: 1, Cooldown: 1, Ejected: 1}
	if got := Exclusions(ctx); got == nil || *got != want {
		t.Fatalf("excluded = %+v, want %+v", got, want)
	}
}

func TestFailureCauseUsesExclusions(t *testing.T) {
	host := fmt.Sprintf("cause-excluded-%d.example:443", time.Now().UnixNano())
	a, b := pineNode("er_fc_a"), pineNode("er_fc_b")

	ctx := WithAttempts(context.Background())
	NoteHop(ctx, []*chain.Node{a, b})
	defaultRefusals.add(nodeRouteKey(a), "tcp", host, classPolicy, time.Now())
	defaultRefusals.add(nodeRouteKey(b), "tcp", host, classPolicy, time.Now())
	NoteRefusal(ctx, a, "tcp", host)
	NoteRefusal(ctx, b, "tcp", host)
	if got := FailureCause(ctx, ErrNoRoute); got != CauseVendorPolicy {
		t.Fatalf("policy no_route cause = %q, want %q", got, CauseVendorPolicy)
	}
	// Zero-attempt no_route from route health alone.
	for name, note := range map[string]func(context.Context, ...*chain.Node){"cooldown": NoteCooldown, "ejected": NoteEjected} {
		ctx = WithAttempts(context.Background())
		NoteHop(ctx, []*chain.Node{a, b})
		note(ctx, a, b)
		if got := FailureCause(ctx, ErrNoRoute); got != CauseNetwork {
			t.Fatalf("%s-only no_route cause = %q, want %q", name, got, CauseNetwork)
		}
	}
	// A canceled request proves nothing, whatever was excluded.
	canceled, cancel := context.WithCancel(context.Background())
	ctx = WithAttempts(canceled)
	NoteHop(ctx, []*chain.Node{a, b})
	NoteCooldown(ctx, a, b)
	cancel()
	if got := FailureCause(ctx, context.Canceled); got != CauseUnknown {
		t.Fatalf("canceled cause = %q, want %q", got, CauseUnknown)
	}
	// A successful request keeps its failover cause.
	ctx = WithAttempts(context.Background())
	NoteHop(ctx, []*chain.Node{a, b})
	NoteCooldown(ctx, b)
	RecordAttempt(ctx, a, "tcp", "site:443", nil)
	if got := FailureCause(ctx, nil); got != "" {
		t.Fatalf("success cause = %q, want none", got)
	}
	// A cause the attempts prove is not changed.
	ctx = WithAttempts(context.Background())
	NoteHop(ctx, []*chain.Node{a, b})
	MarkTried(ctx, a)
	RecordAttempt(ctx, a, "tcp", "site:443", replyError(2))
	NoteCooldown(ctx, b)
	if got := FailureCause(ctx, errors.New("failed")); got != CauseVendorPolicy {
		t.Fatalf("policy attempt cause = %q, want %q", got, CauseVendorPolicy)
	}
}
