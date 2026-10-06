package pineroute

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestMerger(limit int) (*noRouteMerger, *[]func()) {
	var timers []func()
	merger := newNoRouteMerger(limit, noRouteMergeWindow)
	merger.after = func(_ time.Duration, f func()) { timers = append(timers, f) }
	return merger, &timers
}

func TestNoRouteBurstIsReportedTwice(t *testing.T) {
	merger, timers := newTestMerger(8)
	var flushed []int
	var last any
	flush := func(payload any, n int) { flushed, last = append(flushed, n), payload }
	reported := 0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if merger.merge("tcp", "Help.PeacockTV.com.:443", i, flush) {
				mu.Lock()
				reported++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if reported != 1 || len(*timers) != 1 {
		t.Fatalf("reported %d now with %d windows, want 1", reported, len(*timers))
	}
	(*timers)[0]()
	if len(flushed) != 1 || flushed[0] != 999 || last == nil {
		t.Fatalf("flushed %v (last %v), want one summary of 999", flushed, last)
	}
	if !merger.merge("tcp", "help.peacocktv.com:443", "next", flush) {
		t.Fatal("a failure after the window closed was not reported")
	}
}

func TestNoRouteWindowWithoutRepeatsFlushesNothing(t *testing.T) {
	merger, timers := newTestMerger(8)
	called := false
	merger.merge("tcp", "once.example:443", 1, func(any, int) { called = true })
	(*timers)[0]()
	if called {
		t.Fatal("a window without suppressed failures produced a summary")
	}
}

func TestNoRouteWindowsAreKeyedByDestination(t *testing.T) {
	merger, _ := newTestMerger(8)
	flush := func(any, int) {}
	for _, address := range []string{"a.example:443", "a.example:80", "b.example:443", "[2001:db8::1]:443"} {
		if !merger.merge("tcp", address, nil, flush) {
			t.Fatalf("first failure for %s was merged into another destination", address)
		}
	}
	if merger.merge("tcp", "[2001:0db8::1]:443", nil, flush) {
		t.Fatal("equivalent IPv6 spellings opened two windows")
	}
	if !merger.merge("udp", "a.example:443", nil, flush) || !merger.merge("tcp", "bad-address", nil, flush) {
		t.Fatal("an unmergeable failure was suppressed")
	}
}

func TestNoRouteMergerStaysBounded(t *testing.T) {
	merger, _ := newTestMerger(4)
	flush := func(any, int) {}
	for i := 0; i < 10; i++ {
		if !merger.merge("tcp", fmt.Sprintf("h%d.example:443", i), nil, flush) {
			t.Fatal("a first failure was suppressed")
		}
	}
	if len(merger.windows) != 4 {
		t.Fatalf("windows=%d, want 4", len(merger.windows))
	}
	if !merger.merge("tcp", "h9.example:443", nil, flush) {
		t.Fatal("a failure without a window was suppressed when the merger was full")
	}
}

func TestNoRouteMergeDisabledOutsidePine(t *testing.T) {
	Enabled = false
	defer func() { Enabled = true }()
	for i := 0; i < 3; i++ {
		if !MergeNoRoute("tcp", "x.example:443", nil, func(any, int) {}) {
			t.Fatal("no-route failures were merged outside Pine")
		}
	}
}
