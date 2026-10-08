package hop

import (
	"testing"

	"github.com/go-gost/x/config"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
)

type affinityReporter interface{ AffinityForTest() *pineroute.Affinity }

func TestParseHopTurnsAffinityOnOnlyForFIFO(t *testing.T) {
	previous := pineroute.Enabled
	pineroute.Enabled = true
	t.Cleanup(func() { pineroute.Enabled = previous })
	metadata := map[string]any{"pine_site_affinity": true, "pine_plan_tz": "America/New_York", "pine_login_sites": []any{"accounts.example.com"}}
	for strategy, want := range map[string]bool{"fifo": true, "round": false, "hash": false} {
		h, err := ParseHop(&config.HopConfig{Name: "provider", Selector: &config.SelectorConfig{Strategy: strategy}, Metadata: metadata}, xlogger.Nop())
		if err != nil {
			t.Fatal(err)
		}
		affinity := h.(affinityReporter).AffinityForTest()
		if (affinity != nil) != want {
			t.Fatalf("%s: affinity on = %v, want %v", strategy, affinity != nil, want)
		}
		if want && !affinity.LoginSite("accounts.example.com:443") {
			t.Fatal("login list not parsed")
		}
	}
	withMatcher := &config.HopConfig{Selector: &config.SelectorConfig{Strategy: "fifo"},
		Nodes: []*config.NodeConfig{{Name: "plain"}, {Name: "matched", Matcher: &config.NodeMatcherConfig{Rule: "Host(`a.example`)"}}}}
	if affinityCompatible(withMatcher) {
		t.Fatal("hop with a node matcher accepted for affinity")
	}
	if affinityCompatible(&config.HopConfig{Nodes: []*config.NodeConfig{{Name: "plain"}}}) {
		t.Fatal("hop on the default round-robin selector accepted")
	}
	if !affinityCompatible(&config.HopConfig{Selector: &config.SelectorConfig{Strategy: "ha"}}) {
		t.Fatal("ha (fifo) refused")
	}
	h, err := ParseHop(&config.HopConfig{Name: "provider", Selector: &config.SelectorConfig{Strategy: "fifo"}}, xlogger.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if h.(affinityReporter).AffinityForTest() != nil {
		t.Fatal("affinity on without metadata")
	}
}
