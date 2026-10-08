package hop

import (
	"testing"

	"github.com/go-gost/x/config"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
)

type stickyReporter interface{ StickyForTest() *pineroute.Sticky }

func TestParseHopTurnsStickyOnOnlyForFIFO(t *testing.T) {
	previous := pineroute.Enabled
	pineroute.Enabled = true
	t.Cleanup(func() { pineroute.Enabled = previous })
	metadata := map[string]any{"pine_sticky_route": true, "pine_plan_tz": "America/New_York"}
	for strategy, want := range map[string]bool{"fifo": true, "ha": true, "round": false, "hash": false} {
		h, err := ParseHop(&config.HopConfig{Name: "provider", Selector: &config.SelectorConfig{Strategy: strategy}, Metadata: metadata}, xlogger.Nop())
		if err != nil {
			t.Fatal(err)
		}
		if got := h.(stickyReporter).StickyForTest() != nil; got != want {
			t.Fatalf("%s: sticky on = %v, want %v", strategy, got, want)
		}
	}
	withMatcher := &config.HopConfig{Selector: &config.SelectorConfig{Strategy: "fifo"},
		Nodes: []*config.NodeConfig{{Name: "plain"}, {Name: "matched", Matcher: &config.NodeMatcherConfig{Rule: "Host(`a.example`)"}}}}
	if stickyCompatible(withMatcher) {
		t.Fatal("hop with a node matcher accepted")
	}
	if stickyCompatible(&config.HopConfig{Nodes: []*config.NodeConfig{{Name: "plain"}}}) {
		t.Fatal("hop on the default round-robin selector accepted")
	}
	for name, md := range map[string]map[string]any{
		"no metadata":        nil,
		"no chrome timezone": {"pine_sticky_route": true},
		"switch off":         {"pine_sticky_route": false, "pine_plan_tz": "America/New_York"},
	} {
		h, err := ParseHop(&config.HopConfig{Name: "provider", Selector: &config.SelectorConfig{Strategy: "fifo"}, Metadata: md}, xlogger.Nop())
		if err != nil {
			t.Fatal(err)
		}
		if h.(stickyReporter).StickyForTest() != nil {
			t.Fatalf("%s: sticky on", name)
		}
	}
}
