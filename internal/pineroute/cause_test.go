package pineroute

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/go-gost/core/chain"
	mdx "github.com/go-gost/x/metadata"
)

func directNode() *chain.Node {
	return chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
}

func TestFailureCauseRules(t *testing.T) {
	a := routeKey{managedID: "er_a"}
	b := routeKey{managedID: "er_b"}
	c := routeKey{managedID: "er_c"}
	ok := func(route routeKey) attemptRecord { return attemptRecord{route: route, dialed: "tcp/site:443"} }
	reply := func(route routeKey, code uint8) attemptRecord {
		return attemptRecord{route: route, dialed: "tcp/site:443", failed: true, reply: code, destination: true}
	}
	transport := func(route routeKey) attemptRecord {
		return attemptRecord{route: route, dialed: "tcp/site:443", failed: true}
	}
	cases := map[string]struct {
		records   []attemptRecord
		succeeded bool
		canceled  bool
		want      string
	}{
		"first attempt succeeded":       {[]attemptRecord{ok(a)}, true, false, ""},
		"vendor policy then success":    {[]attemptRecord{reply(a, 2), ok(b)}, true, false, CauseVendorPolicy},
		"refused then success":          {[]attemptRecord{reply(a, 5), ok(b)}, true, false, CauseIPRefusedBySite},
		"host unreachable then success": {[]attemptRecord{reply(a, 4), ok(b)}, true, false, CauseRoute},
		"network unreachable":           {[]attemptRecord{reply(a, 3), ok(b)}, true, false, CauseRoute},
		"ttl expired":                   {[]attemptRecord{reply(a, 6), ok(b)}, true, false, CauseRoute},
		"transport then success":        {[]attemptRecord{transport(a), ok(b)}, true, false, CauseNetwork},
		"general failure then success":  {[]attemptRecord{reply(a, 1), ok(b)}, true, false, CauseNetwork},
		"first failure decides":         {[]attemptRecord{reply(a, 5), reply(b, 4), ok(c)}, true, false, CauseIPRefusedBySite},
		"same dialed address preferred": {
			[]attemptRecord{{route: a, dialed: "tcp/other:443", failed: true, reply: 4, destination: true}, reply(b, 5), ok(c)},
			true, false, CauseIPRefusedBySite,
		},
		"only a different address failed": {
			[]attemptRecord{{route: a, dialed: "tcp/other:443", failed: true, reply: 5, destination: true}, ok(b)},
			true, false, CauseUnknown,
		},
		"every route refused by policy": {[]attemptRecord{reply(a, 2), reply(b, 2)}, false, false, CauseVendorPolicy},
		"single route policy":           {[]attemptRecord{reply(a, 2)}, false, false, CauseVendorPolicy},
		"every route reached site":      {[]attemptRecord{reply(a, 4), reply(b, 5), reply(c, 2)}, false, false, CauseSite},
		"single route failed":           {[]attemptRecord{reply(a, 4)}, false, false, CauseUnknown},
		"same route twice":              {[]attemptRecord{reply(a, 4), reply(a, 4)}, false, false, CauseUnknown},
		"every route transport failure": {[]attemptRecord{transport(a), transport(b)}, false, false, CauseNetwork},
		"site with a broken route":      {[]attemptRecord{transport(a), reply(b, 4)}, false, false, CauseSite},
		"policy and transport only":     {[]attemptRecord{reply(a, 2), transport(b)}, false, false, CauseUnknown},
		"no attempt":                    {nil, false, false, CauseUnknown},
		"caller canceled":               {[]attemptRecord{reply(a, 4), reply(b, 4)}, false, true, CauseUnknown},
	}
	for name, tc := range cases {
		if got := failureCause(tc.records, tc.succeeded, tc.canceled); got != tc.want {
			t.Errorf("%s: cause = %q, want %q", name, got, tc.want)
		}
	}
}

func TestRecordAttemptClassifiesErrorsAndIgnoresUntrackedRequests(t *testing.T) {
	RecordAttempt(context.Background(), pineNode("er_untracked"), "tcp", "site:443", replyError(4))
	if got := FailureCause(context.Background(), errors.New("failed")); got != "" {
		t.Fatalf("untracked request cause = %q", got)
	}

	ctx := WithAttempts(context.Background())
	RecordAttempt(ctx, pineNode("er_a"), "tcp", "site:443", UpstreamFailure(replyError(5)))
	RecordAttempt(ctx, pineNode("er_b"), "tcp", "site:443", nil)
	// A reply from an intermediate hop is a route failure, not the site.
	if got := FailureCause(ctx, nil); got != CauseNetwork {
		t.Fatalf("upstream reply cause = %q, want %q", got, CauseNetwork)
	}

	ctx = WithAttempts(context.Background())
	RecordAttempt(ctx, pineNode("er_a"), "tcp", "site:443", replyError(4))
	RecordAttempt(ctx, directNode(), "tcp", "site:443", &net.DNSError{Err: "no such host", Name: "site", IsNotFound: true})
	if got := FailureCause(ctx, errors.New("failed")); got != CauseSite {
		t.Fatalf("managed + direct destination failure cause = %q, want %q", got, CauseSite)
	}

	ctx = WithAttempts(context.Background())
	RecordAttempt(ctx, pineNode("er_a"), "tcp", "site:443", errors.New("i/o timeout"))
	RecordAttempt(ctx, directNode(), "tcp", "site:443", context.DeadlineExceeded)
	if got := FailureCause(ctx, context.DeadlineExceeded); got != CauseNetwork {
		t.Fatalf("all-timeout cause = %q, want %q", got, CauseNetwork)
	}

	// Local network errors on the direct dial are not evidence about the site.
	for _, local := range []error{syscall.ENETUNREACH, syscall.ECONNRESET, errors.New("dial tcp: no route")} {
		ctx = WithAttempts(context.Background())
		RecordAttempt(ctx, pineNode("er_a"), "tcp", "site:443", errors.New("proxy dial failed"))
		RecordAttempt(ctx, directNode(), "tcp", "site:443", &net.OpError{Op: "dial", Err: local})
		if got := FailureCause(ctx, local); got != CauseNetwork {
			t.Fatalf("direct %v cause = %q, want %q", local, got, CauseNetwork)
		}
	}
	ctx = WithAttempts(context.Background())
	RecordAttempt(ctx, pineNode("er_a"), "tcp", "site:443", errors.New("proxy dial failed"))
	RecordAttempt(ctx, directNode(), "tcp", "site:443", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED})
	if got := FailureCause(ctx, syscall.ECONNREFUSED); got != CauseSite {
		t.Fatalf("direct refused cause = %q, want %q", got, CauseSite)
	}
}
