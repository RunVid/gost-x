package hop

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
)

// lastRequest returns the latest request event events captured for host.
func lastRequest(t *testing.T, events *eventLog, host string) pineevent.Event {
	t.Helper()
	requests := events.match(func(event pineevent.Event) bool {
		return event.Kind == "request" && event.DestinationHost == host
	})
	if len(requests) == 0 {
		t.Fatalf("no request event for %s", host)
	}
	return requests[len(requests)-1]
}

func wantRequest(t *testing.T, event pineevent.Event, attempts int, cause string, excluded *pineroute.Excluded) {
	t.Helper()
	if event.Attempts != attempts || event.FailureCause != cause ||
		(event.Excluded == nil) != (excluded == nil) || excluded != nil && *event.Excluded != *excluded {
		t.Fatalf("request event = %+v (excluded %+v), want attempts %d, cause %q, excluded %+v",
			event, event.Excluded, attempts, cause, excluded)
	}
}

func refusing(host string, err error) *refusingTransport {
	return &refusingTransport{refused: map[string]error{host: err}}
}

// A destination every route refuses is the site's (or the vendor's) failure,
// however the routes were excluded, and never network.
func TestDestinationRefusedOnEveryRouteIsNeverNetwork(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "refused-everywhere-" + id + ".example"
	a := pineNode("er_ra_"+id, refusing(host, socksReply(4)))
	b := pineNode("er_rb_"+id, refusing(host, socksReply(5)))
	direct := directPineNode("direct-r-"+id, refusing(host, socksReply(5)))

	// a alone caches its refusal; the plan then attempts only b and direct.
	_ = dial(t, newTestRouter(a), host+":443")
	r := newTestRouter(a, b, direct)
	if err := dial(t, r, host+":443"); err == nil {
		t.Fatal("refused destination returned a connection")
	}
	wantRequest(t, lastRequest(t, events, host), 2, "site", &pineroute.Excluded{Transient: 1})

	// Every route is cached now: no attempt at all.
	if err := dial(t, r, host+":443"); !errors.Is(err, pineroute.ErrNoRoute) {
		t.Fatalf("want no_route, got %v", err)
	}
	wantRequest(t, lastRequest(t, events, host), 0, "site", &pineroute.Excluded{Transient: 3})
}

func TestPolicyOnlyNoRouteIsVendorPolicy(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "policy-" + id + ".example"
	r := newTestRouter(pineNode("er_pa_"+id, refusing(host, socksReply(2))), pineNode("er_pb_"+id, refusing(host, socksReply(2))))
	_ = dial(t, r, host+":443")
	wantRequest(t, lastRequest(t, events, host), 2, "vendor_policy", nil)
	if err := dial(t, r, host+":443"); !errors.Is(err, pineroute.ErrNoRoute) {
		t.Fatalf("want no_route, got %v", err)
	}
	wantRequest(t, lastRequest(t, events, host), 0, "vendor_policy", &pineroute.Excluded{Policy: 2})
}

func TestMixedPolicyAndTransientNoRouteIsSite(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "mixed-" + id + ".example"
	r := newTestRouter(pineNode("er_ma_"+id, refusing(host, socksReply(2))), pineNode("er_mb_"+id, refusing(host, socksReply(4))))
	_ = dial(t, r, host+":443")
	wantRequest(t, lastRequest(t, events, host), 2, "site", nil)
	if err := dial(t, r, host+":443"); !errors.Is(err, pineroute.ErrNoRoute) {
		t.Fatalf("want no_route, got %v", err)
	}
	wantRequest(t, lastRequest(t, events, host), 0, "site", &pineroute.Excluded{Policy: 1, Transient: 1})
}

// Once every route is in failure cooldown and the panic attempt failed,
// selection finds no route: the routes, not the site, are at fault.
func TestCooldownNoRouteIsNetwork(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	first, second := "cool-first-"+id+".example", "cool-second-"+id+".example"
	failing := func() *refusingTransport {
		return &refusingTransport{refused: map[string]error{first: socksReply(1), second: socksReply(1)}}
	}
	r := newTestRouter(pineNode("er_ca_"+id, failing()), pineNode("er_cb_"+id, failing()), pineNode("er_cc_"+id, failing()))
	_ = dial(t, r, first+":443")
	if err := dial(t, r, second+":443"); err == nil {
		t.Fatal("cooled routes returned a connection")
	}
	// One panic attempt; the two other routes stay excluded by cooldown.
	wantRequest(t, lastRequest(t, events, second), 1, "network", &pineroute.Excluded{Cooldown: 2})
}

func TestEjectedRouteWithRouteFailureIsNetwork(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	broken := pineNode("er_ej_"+id, &everythingTransport{err: socksReply(4)})
	healthy := &refusingTransport{}
	other := pineNode("er_ok_"+id, healthy)
	r := newTestRouter(broken, other)
	for i := 1; i <= escalationHosts; i++ {
		if err := dial(t, r, fmt.Sprintf("eject-%d-%s.example:443", i, id)); err != nil {
			t.Fatal(err)
		}
	}
	if !pineroute.Ejected(broken) {
		t.Fatal("precondition: broken route is ejected")
	}
	host := "after-eject-" + id + ".example"
	healthy.refused = map[string]error{host: socksReply(1)}
	if err := dial(t, newRouterWithRetries(0, broken, other), host+":443"); err == nil {
		t.Fatal("failed route returned a connection")
	}
	wantRequest(t, lastRequest(t, events, host), 1, "network", &pineroute.Excluded{Ejected: 1})
}

func TestRequestEventExcludedJSON(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "served-" + id + ".example"
	if err := dial(t, newTestRouter(pineNode("er_js_"+id, &refusingTransport{})), host+":443"); err != nil {
		t.Fatal(err)
	}
	served := lastRequest(t, events, host)
	payload, _ := json.Marshal(served)
	if served.Excluded != nil || strings.Contains(string(payload), "excluded") {
		t.Fatalf("served request without exclusions carries excluded: %s", payload)
	}

	host = "json-policy-" + id + ".example"
	r := newTestRouter(pineNode("er_jp_"+id, refusing(host, socksReply(2))))
	_ = dial(t, r, host+":443")
	_ = dial(t, r, host+":443")
	payload, _ = json.Marshal(lastRequest(t, events, host))
	if !strings.Contains(string(payload), `"excluded":{"policy":1}`) {
		t.Fatalf("no_route payload %s lacks excluded policy count", payload)
	}
}

func newRouterWithRetries(retries int, nodes ...*corechain.Node) *xchain.Router {
	r := newTestRouter(nodes...)
	r.Options().Retries = retries
	return r
}
