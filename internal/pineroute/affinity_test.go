package pineroute

import (
	"testing"
	"time"
)

func TestSiteKey(t *testing.T) {
	cases := map[string]string{
		"accounts.google.com:443":   "google.com",
		"www.google.com:443":        "google.com",
		"Accounts.Google.COM.:443":  "google.com",
		"google.com":                "google.com",
		"a.b.example.co.uk:443":     "example.co.uk",
		"user.github.io:443":        "user.github.io",
		"192.0.2.10:443":            "192.0.2.10",
		"[2001:0db8::1]:443":        "2001:db8::1",
		"[::ffff:192.0.2.1]:443":    "192.0.2.1",
		"localhost:8080":            "localhost",
		"co.uk:443":                 "co.uk",
		"":                          "",
		"intranet.corp.invalid:443": "corp.invalid",
	}
	for address, want := range cases {
		if got := SiteKey(address); got != want {
			t.Errorf("SiteKey(%q) = %q, want %q", address, got, want)
		}
	}
}

func TestPinTableFirstPinAndCompareAndSwap(t *testing.T) {
	table := newPinTable(8, time.Hour)
	key := pinKey{site: "example.com"}
	now := time.Now()
	if !table.swap(key, 0, "er_a", now) {
		t.Fatal("first pin failed")
	}
	_, genA := table.get(key, now)
	if table.swap(key, 0, "er_b", now) {
		t.Fatal("a second first-success pin replaced the existing pin")
	}
	if !table.swap(key, genA, "er_b", now) {
		t.Fatal("compare-and-swap from the current pin failed")
	}
	route, genB := table.get(key, now)
	if route != "er_b" || genB == genA {
		t.Fatalf("pin = %q generation %d", route, genB)
	}
	// A request that saw er_a cannot act on the er_b pin...
	if table.swap(key, genA, "er_c", now) {
		t.Fatal("stale compare-and-swap moved the pin")
	}
	// ...not even after the site went back to er_a (A -> B -> A).
	table.swap(key, genB, "er_a", now)
	route, genA2 := table.get(key, now)
	if route != "er_a" || genA2 == genA {
		t.Fatalf("re-pin to er_a reused generation %d", genA2)
	}
	if table.swap(key, genA, "er_c", now) {
		t.Fatal("stale request moved an A -> B -> A pin")
	}
	table.fail(key, genA, now)
	if failures, _ := table.streak(key, genA2); failures != 0 {
		t.Fatal("stale request extended the new pin's failure streak")
	}
	if _, gen := table.get(pinKey{site: "example.com", login: true}, now); gen != 0 {
		t.Fatal("login key shares the non-login pin")
	}
}

func TestPinTableKeepingRouteKeepsGeneration(t *testing.T) {
	table := newPinTable(8, time.Hour)
	key := pinKey{site: "example.com"}
	now := time.Now()
	table.swap(key, 0, "er_a", now)
	_, gen := table.get(key, now)
	if !table.swap(key, gen, "er_a", now.Add(time.Minute)) {
		t.Fatal("refresh failed")
	}
	if _, again := table.get(key, now); again != gen {
		t.Fatal("refresh changed the generation")
	}
}

func TestPinTableLRUBound(t *testing.T) {
	table := newPinTable(3, time.Hour)
	now := time.Now()
	for _, site := range []string{"a.com", "b.com", "c.com"} {
		table.swap(pinKey{site: site}, 0, "er_"+site, now)
	}
	// Use a.com so b.com becomes the least recently used.
	_, gen := table.get(pinKey{site: "a.com"}, now)
	table.swap(pinKey{site: "a.com"}, gen, "er_a.com", now.Add(time.Second))
	table.swap(pinKey{site: "d.com"}, 0, "er_d.com", now.Add(2*time.Second))
	if table.lru.Len() != 3 {
		t.Fatalf("len = %d", table.lru.Len())
	}
	if _, gen := table.get(pinKey{site: "b.com"}, now.Add(3*time.Second)); gen != 0 {
		t.Fatal("least recently used pin was not evicted")
	}
	for _, site := range []string{"a.com", "c.com", "d.com"} {
		if _, gen := table.get(pinKey{site: site}, now.Add(3*time.Second)); gen == 0 {
			t.Fatalf("%s evicted", site)
		}
	}
}

func TestPinTableSlidingTTL(t *testing.T) {
	table := newPinTable(8, time.Hour)
	key := pinKey{site: "example.com"}
	now := time.Now()
	table.swap(key, 0, "er_a", now)
	_, gen := table.get(key, now)
	// A success refreshes the pin; a failure does not.
	table.swap(key, gen, "er_a", now.Add(50*time.Minute))
	table.fail(key, gen, now.Add(100*time.Minute))
	if _, g := table.get(key, now.Add(109*time.Minute)); g == 0 {
		t.Fatal("pin used 59 minutes ago expired")
	}
	if _, g := table.get(key, now.Add(110*time.Minute)); g != 0 {
		t.Fatal("pin unused for the TTL did not expire")
	}
	if table.lru.Len() != 0 {
		t.Fatal("expired pin kept")
	}
	// An expired pin counts as unpinned for compare-and-swap, and a request
	// that saw it cannot act on the new one.
	if table.swap(key, gen, "er_b", now.Add(111*time.Minute)) {
		t.Fatal("request that saw the expired pin re-pinned")
	}
	if !table.swap(key, 0, "er_b", now.Add(111*time.Minute)) {
		t.Fatal("could not pin after expiry")
	}
}

func TestPinTableFailureStreak(t *testing.T) {
	table := newPinTable(8, time.Hour)
	key := pinKey{site: "example.com", login: true}
	now := time.Now()
	table.swap(key, 0, "er_a", now)
	_, gen := table.get(key, now)
	table.fail(key, gen+100, now) // another pin's request
	if failures, _ := table.streak(key, gen); failures != 0 {
		t.Fatal("failure for another pin counted")
	}
	table.fail(key, gen, now.Add(time.Second))
	table.fail(key, gen, now.Add(2*time.Second))
	failures, since := table.streak(key, gen)
	if failures != 2 || !since.Equal(now.Add(time.Second)) {
		t.Fatalf("streak = %d since %v", failures, since)
	}
	table.swap(key, gen, "er_a", now.Add(3*time.Second))
	if failures, _ := table.streak(key, gen); failures != 0 {
		t.Fatal("success did not clear the streak")
	}
}

func TestPinTableDisabled(t *testing.T) {
	table := newPinTable(0, time.Hour)
	if table.swap(pinKey{site: "example.com"}, 0, "er_a", time.Now()) {
		t.Fatal("pinned with a zero limit")
	}
}

func TestAffinityLoginSiteMatching(t *testing.T) {
	affinity := NewAffinity("America/New_York", []string{"accounts.example.com", ".login.example.org", "mixed.example.net"})
	cases := map[string]bool{
		"accounts.example.com:443":  true,
		"Accounts.Example.COM.:443": false, // compared as sent, like GOST's matcher
		"www.example.com:443":       false,
		"example.com:443":           false,
		"login.example.org:443":     true,
		"a.b.login.example.org:443": true,
		"xlogin.example.org:443":    false,
		"mixed.example.net":         true,
		"sub.mixed.example.net:443": false,
		"":                          false,
		"[2001:db8::1]:443":         false,
	}
	for host, want := range cases {
		if got := affinity.LoginSite(host); got != want {
			t.Errorf("LoginSite(%q) = %v, want %v", host, got, want)
		}
	}
	var none *Affinity
	if none.LoginSite("accounts.example.com:443") {
		t.Fatal("nil affinity has login sites")
	}
	if NewAffinity("", []string{"accounts.example.com"}) != nil {
		t.Fatal("affinity without Chrome's timezone")
	}
}

func TestLoginHoldExpires(t *testing.T) {
	previousHold, previousPins := loginHold, defaultPins
	defer func() { loginHold, defaultPins = previousHold, previousPins }()
	loginHold = time.Minute
	defaultPins = newPinTable(8, time.Hour)
	key := pinKey{site: "example.com", login: true}
	now := time.Now()
	defaultPins.swap(key, 0, "er_a", now)
	route, gen := defaultPins.get(key, now)
	selection := func() *SiteSelection {
		return &SiteSelection{key: key, pinned: route, generation: gen}
	}
	for i := 0; i < loginHoldFailures; i++ {
		defaultPins.fail(key, gen, now.Add(-2*time.Minute))
	}
	if selection().Hold() {
		t.Fatal("pin failing for longer than the hold window is still held")
	}
	// Fewer failures, or a shorter streak, still hold.
	defaultPins.swap(key, gen, "er_a", now)
	defaultPins.fail(key, gen, now.Add(-2*time.Minute))
	if !selection().Hold() {
		t.Fatal("one failure released the hold")
	}
	defaultPins.swap(key, gen, "er_a", now)
	for i := 0; i < loginHoldFailures; i++ {
		defaultPins.fail(key, gen, now)
	}
	if !selection().Hold() {
		t.Fatal("a fresh streak released the hold")
	}
	nonLogin := &SiteSelection{key: pinKey{site: "example.com"}, pinned: route, generation: gen}
	if nonLogin.Hold() {
		t.Fatal("non-login site held")
	}
}

func TestPinTableRemoveIsCompareAndSwap(t *testing.T) {
	table := newPinTable(8, time.Hour)
	key := pinKey{site: "example.com"}
	now := time.Now()
	table.swap(key, 0, "er_a", now)
	_, gen := table.get(key, now)
	if table.remove(key, gen+1, now) {
		t.Fatal("stale remove dropped the pin")
	}
	if !table.remove(key, gen, now) || table.lru.Len() != 0 {
		t.Fatal("remove of the current pin failed")
	}
	if table.remove(key, gen, now) {
		t.Fatal("removed twice")
	}
}

// A route-wide ejection is reported as such even after an earlier reason.
func TestMoveReportsEjection(t *testing.T) {
	s := &SiteSelection{pinned: "er_a"}
	s.Move(MoveRouteFailed)
	s.Move(MoveSiteRefused)
	if s.reason != MoveRouteFailed {
		t.Fatalf("reason = %q, want the first", s.reason)
	}
	s.Move(MoveRouteEjected)
	if s.reason != MoveRouteEjected {
		t.Fatalf("reason = %q, want route_ejected", s.reason)
	}
}
