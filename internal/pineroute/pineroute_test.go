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
	"github.com/go-gost/core/metadata"
	mdx "github.com/go-gost/x/metadata"
)

func TestMain(m *testing.M) {
	Enabled = true
	m.Run()
}

func pineNode(id string) *chain.Node {
	var md metadata.Metadata = mdx.NewMetadata(map[string]any{"pine_route_id": id})
	return chain.NewNode(id, "proxy.example:1080", chain.MetadataNodeOption(md))
}

type replyError uint8

func (e replyError) Error() string          { return fmt.Sprintf("reply %d", uint8(e)) }
func (e replyError) SOCKS5ReplyCode() uint8 { return uint8(e) }

func TestDestinationScoped(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"not allowed":        {fmt.Errorf("wrapped: %w", replyError(2)), true},
		"connection refused": {replyError(5), true},
		"general failure":    {replyError(1), false},
		"host unreachable":   {replyError(4), true},
		"plain error":        {errors.New("dial tcp: i/o timeout"), false},
		"nil":                {nil, false},
	}
	for name, tc := range cases {
		if got := DestinationScoped("tcp", tc.err); got != tc.want {
			t.Errorf("%s: DestinationScoped = %v, want %v", name, got, tc.want)
		}
	}
	if DestinationScoped("udp", replyError(2)) {
		t.Error("a UDP reply was treated as a destination refusal")
	}
}

func TestSkipTracksTriedNodesPerRequest(t *testing.T) {
	node := pineNode("er_route_a")
	if Skip(context.Background(), node, "tcp", "example.com:443") {
		t.Fatal("untracked request skipped a node")
	}
	first := WithAttempts(context.Background())
	MarkTried(first, node)
	if !Skip(first, node, "tcp", "example.com:443") {
		t.Fatal("tried node was not skipped for the same request")
	}
	if Skip(WithAttempts(context.Background()), node, "tcp", "example.com:443") {
		t.Fatal("tried node leaked into another request")
	}
	if Skip(first, chain.NewNode("direct-fallback", ""), "tcp", "example.com:443") {
		t.Fatal("a node without a Pine route id was skipped")
	}
}

func TestRefusalCacheScopesByNodeAndHost(t *testing.T) {
	cache := newRefusalCache(8)
	now := time.Unix(1_700_000_000, 0)
	cache.add(routeKey{managedID: "route-a"}, "tcp", "Accounts.Google.com.:443", time.Minute, now)

	if !cache.contains(routeKey{managedID: "route-a"}, "tcp", "accounts.google.com:443", now) {
		t.Fatal("refused host was not remembered")
	}
	if cache.contains(routeKey{managedID: "route-a"}, "tcp", "example.com:443", now) {
		t.Fatal("refusal spread to another host")
	}
	if cache.contains(routeKey{managedID: "route-b"}, "tcp", "accounts.google.com:443", now) {
		t.Fatal("refusal spread to another route")
	}
	if cache.contains(routeKey{managedID: "route-a"}, "tcp", "accounts.google.com:443", now.Add(time.Minute)) {
		t.Fatal("refusal outlived its TTL")
	}
}

func TestRefusalCacheStaysBounded(t *testing.T) {
	cache := newRefusalCache(4)
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 10; i++ {
		cache.add(routeKey{managedID: "route-a"}, "tcp", fmt.Sprintf("host-%d.example:443", i), time.Hour, now)
	}
	if len(cache.entries) > 4 {
		t.Fatalf("cache holds %d entries, want at most 4", len(cache.entries))
	}

	cache = newRefusalCache(2)
	cache.add(routeKey{managedID: "route-a"}, "tcp", "old.example:443", time.Hour, now)
	cache.add(routeKey{managedID: "route-a"}, "tcp", "kept.example:443", time.Hour, now.Add(time.Minute))
	cache.add(routeKey{managedID: "route-a"}, "tcp", "new.example:443", time.Hour, now.Add(2*time.Minute))
	if cache.contains(routeKey{managedID: "route-a"}, "tcp", "old.example:443", now.Add(2*time.Minute)) {
		t.Fatal("a full cache kept its oldest refusal")
	}
	if !cache.contains(routeKey{managedID: "route-a"}, "tcp", "kept.example:443", now.Add(2*time.Minute)) {
		t.Fatal("a full cache dropped a newer live refusal")
	}
}

func TestNestedDialGetsFreshAttempts(t *testing.T) {
	node := pineNode("er_route_nested")
	outer := WithAttempts(context.Background())
	MarkTried(outer, node)
	if Skip(WithAttempts(outer), node, "tcp", "example.com:443") {
		t.Fatal("a nested dial inherited the outer request's attempts")
	}
}

func TestDisabledOutsidePine(t *testing.T) {
	Enabled = false
	defer func() { Enabled = true }()
	if DestinationScoped("tcp", replyError(2)) {
		t.Fatal("a refusal was scoped to the destination outside Pine")
	}
	if Tracking(WithAttempts(context.Background())) {
		t.Fatal("attempts were tracked outside Pine")
	}
}

func TestDestinationReplyClassification(t *testing.T) {
	for code := 0; code <= 255; code++ {
		want := code == 2 || code == 3 || code == 4 || code == 5 || code == 6 || code == 8
		for _, network := range []string{"tcp", "tcp4", "tcp6"} {
			if got := DestinationScoped(network, fmt.Errorf("wrapped: %w", replyError(code))); got != want {
				t.Fatalf("network=%s reply=%d scoped=%v, want %v", network, code, got, want)
			}
		}
	}
}

func TestRefusalCacheSeparatesPortsNetworksAndIPFamilies(t *testing.T) {
	cache := newRefusalCache(8)
	now := time.Now()
	cache.add(routeKey{managedID: "route"}, "tcp", "Example.COM.:0443", time.Minute, now)
	if !cache.contains(routeKey{managedID: "route"}, "tcp", "example.com:443", now) {
		t.Fatal("equivalent hostname/port did not match")
	}
	for _, endpoint := range []struct{ network, address string }{
		{"tcp", "example.com:80"}, {"tcp4", "example.com:443"}, {"tcp6", "example.com:443"},
	} {
		if cache.contains(routeKey{managedID: "route"}, endpoint.network, endpoint.address, now) {
			t.Fatalf("refusal crossed endpoint boundary: %+v", endpoint)
		}
	}
	cache.add(routeKey{managedID: "route"}, "tcp", "[2001:0db8::1]:443", time.Minute, now)
	if !cache.contains(routeKey{managedID: "route"}, "tcp", "[2001:db8::1]:443", now) {
		t.Fatal("equivalent IPv6 address did not match")
	}
	for _, address := range []string{"", "example.com", ":443", "example.com:0", "example.com:65536"} {
		cache.add(routeKey{managedID: "invalid"}, "tcp", address, time.Minute, now)
		if cache.contains(routeKey{managedID: "invalid"}, "tcp", address, now) {
			t.Fatalf("invalid endpoint cached: %q", address)
		}
	}
}

func TestTransientRefusalExpiresWithoutSlidingOnReads(t *testing.T) {
	cache := newRefusalCache(8)
	now := time.Now()
	cache.add(routeKey{managedID: "route"}, "tcp", "gone.example:443", transientTTL, now)
	if !cache.contains(routeKey{managedID: "route"}, "tcp", "gone.example:443", now.Add(transientTTL-time.Nanosecond)) {
		t.Fatal("transient refusal expired too early")
	}
	if cache.contains(routeKey{managedID: "route"}, "tcp", "gone.example:443", now.Add(transientTTL)) {
		t.Fatal("cache reads prolonged a transient refusal")
	}
}

func TestDirectAndCancellationIsolation(t *testing.T) {
	node := chain.NewNode("direct-fallback", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	ctx := WithAttempts(context.Background())
	MarkTried(ctx, node)
	if !Skip(ctx, node, "tcp", "example.com:443") {
		t.Fatal("explicit direct connector can be retried within one request")
	}
	if Skip(WithAttempts(context.Background()), node, "tcp", "example.com:443") {
		t.Fatal("direct attempt leaked into another request")
	}
	if !IgnoreFailure(context.Background(), node, "tcp", &net.DNSError{IsNotFound: true}) {
		t.Fatal("direct destination DNS error would cool the shared direct route")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	proxy := pineNode("er_canceled")
	if !IgnoreFailure(canceled, proxy, "tcp", errors.New("interrupted dial")) {
		t.Fatal("request cancellation would damage proxy health")
	}
	RecordRefusal(canceled, proxy, "tcp", "example.com:443", replyError(4))
	if Skip(WithAttempts(context.Background()), proxy, "tcp", "example.com:443") {
		t.Fatal("request cancellation polluted the destination cache")
	}
	RecordRefusal(context.Background(), node, "tcp", "missing.example:443", &net.DNSError{IsNotFound: true})
	if !Skip(WithAttempts(context.Background()), node, "tcp", "missing.example:443") ||
		Skip(WithAttempts(context.Background()), node, "tcp", "healthy.example:443") {
		t.Fatal("direct DNS failure was not isolated to its endpoint")
	}
}

func TestRefusalCacheConcurrentChurn(t *testing.T) {
	cache := newRefusalCache(32)
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				address := fmt.Sprintf("host-%d-%d.example:443", worker, i)
				cache.add(routeKey{managedID: "route"}, "tcp", address, transientTTL, time.Now())
				cache.contains(routeKey{managedID: "route"}, "tcp", address, time.Now())
			}
		}(worker)
	}
	wg.Wait()
	if len(cache.entries) > 32 {
		t.Fatalf("concurrent churn exceeded the cache bound: %d", len(cache.entries))
	}
}

func TestUpstreamFailurePreservesErrorIdentityAndScope(t *testing.T) {
	cause := replyError(4)
	err := fmt.Errorf("outer: %w", UpstreamFailure(cause))
	var reply socks5ReplyError
	if !errors.Is(err, cause) || !errors.As(err, &reply) || reply.SOCKS5ReplyCode() != 4 {
		t.Fatal("upstream phase marker lost the original error")
	}
	direct := chain.NewNode("direct", "", chain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	if DestinationScoped("tcp", err) || IgnoreFailure(context.Background(), direct, "tcp", err) {
		t.Fatal("upstream error was treated as a destination failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !IgnoreFailure(ctx, direct, "tcp", err) {
		t.Fatal("upstream phase marker hid caller cancellation")
	}
	Enabled = false
	defer func() { Enabled = true }()
	if UpstreamFailure(cause) != cause {
		t.Fatal("upstream wrapping changed non-Pine behavior")
	}
}

func TestOnlyTCPConnectNetworksAreDestinationScoped(t *testing.T) {
	for _, network := range []string{"", "udp", "udp4", "udp6", "unix", "tcp-invalid"} {
		if DestinationScoped(network, replyError(4)) {
			t.Fatalf("unsupported network %q classified as TCP CONNECT", network)
		}
	}
}
