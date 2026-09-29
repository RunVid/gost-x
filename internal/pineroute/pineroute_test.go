package pineroute

import (
	"context"
	"errors"
	"fmt"
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
		"connection refused": {replyError(5), false},
		"general failure":    {replyError(1), false},
		"host unreachable":   {replyError(4), false},
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
	if Skip(context.Background(), node, "example.com:443") {
		t.Fatal("untracked request skipped a node")
	}
	first := WithAttempts(context.Background())
	MarkTried(first, node)
	if !Skip(first, node, "example.com:443") {
		t.Fatal("tried node was not skipped for the same request")
	}
	if Skip(WithAttempts(context.Background()), node, "example.com:443") {
		t.Fatal("tried node leaked into another request")
	}
	if Skip(first, chain.NewNode("direct-fallback", ""), "example.com:443") {
		t.Fatal("a node without a Pine route id was skipped")
	}
}

func TestRefusalCacheScopesByNodeAndHost(t *testing.T) {
	cache := newRefusalCache(time.Minute, 8)
	now := time.Unix(1_700_000_000, 0)
	cache.add("route-a", "Accounts.Google.com.:443", now)

	if !cache.contains("route-a", "accounts.google.com", now) {
		t.Fatal("refused host was not remembered")
	}
	if cache.contains("route-a", "example.com", now) {
		t.Fatal("refusal spread to another host")
	}
	if cache.contains("route-b", "accounts.google.com", now) {
		t.Fatal("refusal spread to another route")
	}
	if cache.contains("route-a", "accounts.google.com", now.Add(time.Minute)) {
		t.Fatal("refusal outlived its TTL")
	}
}

func TestRefusalCacheStaysBounded(t *testing.T) {
	cache := newRefusalCache(time.Hour, 4)
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 10; i++ {
		cache.add("route-a", fmt.Sprintf("host-%d.example", i), now)
	}
	if len(cache.entries) > 4 {
		t.Fatalf("cache holds %d entries, want at most 4", len(cache.entries))
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
