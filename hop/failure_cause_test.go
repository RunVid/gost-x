package hop

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/x/internal/pineevent"
	mdx "github.com/go-gost/x/metadata"
)

// requestCause dials host through routes and returns the failure_cause of the
// request event the router emitted for it.
func requestCause(t *testing.T, host string, routes ...*corechain.Node) (string, pineevent.Event) {
	t.Helper()
	var mu sync.Mutex
	var request *pineevent.Event
	restore := pineevent.CaptureForTest(func(event pineevent.Event) {
		if event.Kind != "request" || event.DestinationHost != host {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		request = &event
	})
	defer restore()
	_ = dial(t, newTestRouter(routes...), host+":443")
	mu.Lock()
	defer mu.Unlock()
	if request == nil {
		t.Fatal("router emitted no request event")
	}
	return request.FailureCause, *request
}

func TestRequestEventCarriesFailureCause(t *testing.T) {
	timeout := context.DeadlineExceeded
	cases := []struct {
		name   string
		routes []error // per route, the CONNECT result for the host; nil connects
		want   string
	}{
		{"first route succeeds", []error{nil}, ""},
		{"vendor policy then success", []error{socksReply(2), nil}, "vendor_policy"},
		{"refused then success", []error{socksReply(5), nil}, "ip_refused_by_site"},
		{"host unreachable then success", []error{socksReply(4), nil}, "route"},
		{"network unreachable then success", []error{socksReply(3), nil}, "route"},
		{"ttl expired then success", []error{socksReply(6), nil}, "route"},
		{"proxy failure then success", []error{socksReply(1), nil}, "network"},
		{"proxy timeout then success", []error{timeout, nil}, "network"},
		{"address type unsupported then success", []error{socksReply(8), nil}, "unknown"},
		{"every route refused by policy", []error{socksReply(2), socksReply(2)}, "vendor_policy"},
		{"no route reaches the site", []error{socksReply(4), socksReply(5), socksReply(4)}, "site"},
		{"every route address type unsupported", []error{socksReply(8), socksReply(8)}, "unknown"},
		{"only one route tried", []error{socksReply(4)}, "unknown"},
		{"every proxy broken", []error{socksReply(1), timeout}, "network"},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := fmt.Sprintf("cause-%d-%d.example", index, time.Now().UnixNano())
			routes := make([]*corechain.Node, 0, len(tc.routes))
			for routeIndex, err := range tc.routes {
				transport := &refusingTransport{}
				if err != nil {
					transport.refused = map[string]error{host: err}
				}
				routes = append(routes, pineNode(fmt.Sprintf("er_%d_%s", routeIndex, host), transport))
			}
			got, event := requestCause(t, host, routes...)
			if got != tc.want {
				t.Fatalf("failure_cause = %q, want %q (event %+v)", got, tc.want, event)
			}
			if tc.want == "" && event.Outcome != "success" {
				t.Fatalf("outcome = %q", event.Outcome)
			}
		})
	}
}

func directPineNode(id string, transport corechain.Transporter) *corechain.Node {
	return corechain.NewNode(id, "",
		corechain.TransportNodeOption(transport),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
}

func TestSingleRouteUpstreamFailureCauseIsNetwork(t *testing.T) {
	host := fmt.Sprintf("upstream-cause-%d.example", time.Now().UnixNano())
	route := pineNode("er_upstream_"+host, &refusingTransport{dialErr: fmt.Errorf("proxy connect failed")})
	if got, event := requestCause(t, host, route); got != "network" {
		t.Fatalf("failure_cause = %q, want network (event %+v)", got, event)
	}
}

func TestRequestExcludedByRefusalCacheIsUnknown(t *testing.T) {
	host := fmt.Sprintf("cached-%d.example", time.Now().UnixNano())
	route := pineNode("er_cached_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4)}})
	router := newTestRouter(route)
	_ = dial(t, router, host+":443")
	var mu sync.Mutex
	var request *pineevent.Event
	restore := pineevent.CaptureForTest(func(event pineevent.Event) {
		if event.Kind == "request" && event.DestinationHost == host {
			mu.Lock()
			request = &event
			mu.Unlock()
		}
	})
	defer restore()
	// The transient refusal is cached for this route, so no attempt is made.
	if err := dial(t, router, host+":443"); err == nil {
		t.Fatal("cached refusal returned a connection")
	}
	mu.Lock()
	defer mu.Unlock()
	if request == nil || request.Attempts != 0 || request.FailureCause != "unknown" {
		t.Fatalf("request event = %+v, want no attempt and cause unknown", request)
	}
}

func TestDirectFallbackFailureCountsTowardSite(t *testing.T) {
	host := fmt.Sprintf("direct-cause-%d.example", time.Now().UnixNano())
	managed := pineNode("er_managed_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4)}})
	direct := corechain.NewNode("direct-fallback", "",
		corechain.TransportNodeOption(&refusingTransport{refused: map[string]error{host: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}}),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	if got, event := requestCause(t, host, managed, direct); got != "site" {
		t.Fatalf("failure_cause = %q, want site (event %+v)", got, event)
	}
}

func TestDirectRefusedThenSuccessCauseIsIPRefusedBySite(t *testing.T) {
	host := fmt.Sprintf("direct-refused-%d.example", time.Now().UnixNano())
	direct := directPineNode("direct-refused-"+host, &refusingTransport{refused: map[string]error{
		host: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
	}})
	managed := pineNode("er_direct-refused-"+host, &refusingTransport{})
	if got, event := requestCause(t, host, direct, managed); got != "ip_refused_by_site" {
		t.Fatalf("failure_cause = %q, want ip_refused_by_site (event %+v)", got, event)
	}
}

func TestDirectDNSNotFoundThenSuccessCauseIsUnknown(t *testing.T) {
	host := fmt.Sprintf("direct-dns-%d.example", time.Now().UnixNano())
	direct := directPineNode("direct-dns-"+host, &refusingTransport{refused: map[string]error{
		host: &net.DNSError{Err: "no such host", Name: host, IsNotFound: true},
	}})
	managed := pineNode("er_direct-dns-"+host, &refusingTransport{})
	if got, event := requestCause(t, host, direct, managed); got != "unknown" {
		t.Fatalf("failure_cause = %q, want unknown (event %+v)", got, event)
	}
}
