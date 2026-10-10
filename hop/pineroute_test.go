package hop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-gost/core/bypass"
	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/routing"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
	mdx "github.com/go-gost/x/metadata"
	xselector "github.com/go-gost/x/selector"
)

func TestMain(m *testing.M) {
	pineroute.Enabled = true
	pineroute.Escalation = true
	m.Run()
}

func pineNode(id string, transport corechain.Transporter) *corechain.Node {
	return corechain.NewNode(id, id+".example:1080",
		corechain.TransportNodeOption(transport),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_id": id})))
}

type socksReply uint8

func (e socksReply) Error() string {
	return fmt.Sprintf("socks5 connect rejected with reply %d", uint8(e))
}
func (e socksReply) SOCKS5ReplyCode() uint8 { return uint8(e) }

type refusingTransport struct {
	refused       map[string]error
	dialErr       error
	handshakeErr  error
	cancelConnect bool
	dials         atomic.Int32
	connects      atomic.Int32
	options       corechain.TransportOptions
}

func (t *refusingTransport) Dial(context.Context, string) (net.Conn, error) {
	t.dials.Add(1)
	if t.dialErr != nil {
		return nil, t.dialErr
	}
	client, peer := net.Pipe()
	go func() {
		defer peer.Close()
		_, _ = peer.Read(make([]byte, 1))
	}()
	return client, nil
}

func (t *refusingTransport) Handshake(_ context.Context, conn net.Conn) (net.Conn, error) {
	return conn, t.handshakeErr
}

func (t *refusingTransport) Connect(ctx context.Context, conn net.Conn, _, address string) (net.Conn, error) {
	t.connects.Add(1)
	if t.cancelConnect {
		<-ctx.Done()
		return conn, ctx.Err()
	}
	if err := t.refused[address]; err != nil {
		return conn, err
	}
	host, _, _ := net.SplitHostPort(address)
	if err := t.refused[host]; err != nil {
		return conn, err
	}
	return conn, nil
}

func (t *refusingTransport) Bind(context.Context, net.Conn, string, string, ...connector.BindOption) (net.Listener, error) {
	return nil, connector.ErrBindUnsupported
}

func (t *refusingTransport) Multiplex() bool                      { return false }
func (t *refusingTransport) Options() *corechain.TransportOptions { return &t.options }
func (t *refusingTransport) Copy() corechain.Transporter          { return t }

func newTestRouter(nodes ...*corechain.Node) *xchain.Router {
	return newTestRouterWithFailFilter(1, 30*time.Second, nodes...)
}

func newTestRouterWithFailFilter(maxFails int, failTimeout time.Duration, nodes ...*corechain.Node) *xchain.Router {
	h := NewHop(NodeOption(nodes...), SelectorOption(xselector.NewSelector(
		xselector.FIFOStrategy[*corechain.Node](),
		xselector.FailFilter[*corechain.Node](maxFails, failTimeout),
		xselector.BackupFilter[*corechain.Node](),
	)))
	c := xchain.NewChain("provider-routes")
	c.AddHop(h)
	return xchain.NewRouter(
		corechain.ChainRouterOption(c),
		corechain.RetriesRouterOption(len(nodes)-1),
		corechain.LoggerRouterOption(xlogger.Nop()),
	)
}

func dial(t *testing.T, r *xchain.Router, address string) error {
	t.Helper()
	conn, err := r.Dial(context.Background(), "tcp", address)
	if conn != nil {
		_ = conn.Close()
	}
	return err
}

func TestDestinationRefusalDoesNotCoolDownRoute(t *testing.T) {
	host := fmt.Sprintf("refused-%d.example", time.Now().UnixNano())
	primary := &refusingTransport{refused: map[string]error{host: socksReply(2)}}
	fallback := &refusingTransport{}
	primaryNode := pineNode("er_primary_"+host, primary)
	fallbackNode := pineNode("er_fallback_"+host, fallback)
	r := newTestRouter(primaryNode, fallbackNode)

	if err := dial(t, r, host+":443"); err != nil {
		t.Fatalf("refused host did not fail over: %v", err)
	}
	if got := primaryNode.Marker().Count(); got != 0 {
		t.Fatalf("primary failure count = %d, want 0 after a destination refusal", got)
	}

	if err := dial(t, r, "allowed.example:443"); err != nil {
		t.Fatal(err)
	}
	if got := fallback.connects.Load(); got != 1 {
		t.Fatalf("fallback CONNECTs = %d, want 1: another host must still use the primary", got)
	}

	before := primary.connects.Load()
	if err := dial(t, r, host+":443"); err != nil {
		t.Fatal(err)
	}
	if got := primary.connects.Load(); got != before {
		t.Fatal("primary was retried for a host it refused moments ago")
	}
}

func TestRouteFailureStillCoolsDownRoute(t *testing.T) {
	host := fmt.Sprintf("broken-%d.example", time.Now().UnixNano())
	primary := &refusingTransport{refused: map[string]error{host: socksReply(1)}}
	primaryNode := pineNode("er_primary_"+host, primary)
	fallbackNode := pineNode("er_fallback_"+host, &refusingTransport{})
	r := newTestRouter(primaryNode, fallbackNode)

	if err := dial(t, r, host+":443"); err != nil {
		t.Fatal(err)
	}
	if got := primaryNode.Marker().Count(); got != 1 {
		t.Fatalf("primary failure count = %d, want 1 after a general failure", got)
	}
}

func TestEveryRouteRefusedFailsWithoutDirectDial(t *testing.T) {
	host := fmt.Sprintf("blocked-%d.example", time.Now().UnixNano())
	a := &refusingTransport{refused: map[string]error{host: socksReply(2)}}
	b := &refusingTransport{refused: map[string]error{host: socksReply(2)}}
	nodeA := pineNode("er_a_"+host, a)
	nodeB := pineNode("er_b_"+host, b)
	r := newTestRouter(nodeA, nodeB)

	if err := dial(t, r, host+":443"); err == nil {
		t.Fatal("a host every route refuses returned a connection")
	}
	err := dial(t, r, host+":443")
	if !errors.Is(err, pineroute.ErrNoRoute) {
		t.Fatalf("second request error = %v, want ErrNoRoute", err)
	}
	if a.connects.Load() != 1 || b.connects.Load() != 1 {
		t.Fatalf("CONNECTs = %d/%d, want each route tried once", a.connects.Load(), b.connects.Load())
	}
	if nodeA.Marker().Count() != 0 || nodeB.Marker().Count() != 0 {
		t.Fatal("destination refusals put routes into cooldown")
	}
}

func TestDestinationFailuresLeaveOtherHostsAndPortsUsable(t *testing.T) {
	for _, reply := range []socksReply{2, 3, 4, 5, 6, 8} {
		t.Run(fmt.Sprint(reply), func(t *testing.T) {
			host := fmt.Sprintf("endpoint-%d-%d.example", reply, time.Now().UnixNano())
			a := &refusingTransport{refused: map[string]error{host + ":80": reply}}
			b := &refusingTransport{refused: map[string]error{host + ":80": reply}}
			nodes := []*corechain.Node{pineNode("er_a_"+host, a), pineNode("er_b_"+host, b)}
			r := newTestRouter(nodes...)
			// A larger retry budget must never repeat an exhausted node or
			// discard the last useful error in favour of generic no_route.
			r.Options().Retries = 8
			if err := dial(t, r, host+":80"); !errors.Is(err, reply) {
				t.Fatalf("error=%v, want reply %d", err, reply)
			}
			if a.connects.Load() != 1 || b.connects.Load() != 1 {
				t.Fatal("route retried in the same request")
			}
			if err := dial(t, r, host+":443"); err != nil {
				t.Fatalf("other port broken: %v", err)
			}
			if err := dial(t, r, "healthy.example:443"); err != nil {
				t.Fatalf("other host broken: %v", err)
			}
			for _, node := range nodes {
				if node.Marker().Count() != 0 {
					t.Fatal("destination failure cooled shared route")
				}
			}
			if err := dial(t, r, host+":80"); !errors.Is(err, pineroute.ErrNoRoute) {
				t.Fatalf("repeat request did not use bounded cache: %v", err)
			}
		})
	}
}

func TestDirectDestinationFailureDoesNotCoolFallback(t *testing.T) {
	for _, failure := range []error{&net.DNSError{IsNotFound: true}, errors.New("connection refused"), context.DeadlineExceeded} {
		host := fmt.Sprintf("direct-%d.example", time.Now().UnixNano())
		a := pineNode("er_a_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4), "healthy.example": socksReply(2)}})
		b := pineNode("er_b_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4), "healthy.example": socksReply(2)}})
		transport := &refusingTransport{refused: map[string]error{host: failure}}
		direct := corechain.NewNode("direct", "", corechain.TransportNodeOption(transport), corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct", "backup": true})))
		r := newTestRouter(a, b, direct)
		r.Options().Retries = 8
		if err := dial(t, r, host+":443"); !errors.Is(err, failure) {
			t.Fatalf("lost destination error: %v", err)
		}
		if transport.connects.Load() != 1 {
			t.Fatal("direct connector retried for one request")
		}
		if direct.Marker().Count() != 0 {
			t.Fatal("destination error cooled direct fallback")
		}
		if err := dial(t, r, "healthy.example:443"); err != nil {
			t.Fatalf("healthy direct destination broken: %v", err)
		}
	}
}

func TestDirectRefusalDoesNotCrossRouterOrReloadBoundary(t *testing.T) {
	host := fmt.Sprintf("independent-direct-%d.example:443", time.Now().UnixNano())
	failure := errors.New("destination unavailable on this direct connector")
	newDirect := func(transport *refusingTransport) *corechain.Node {
		// Names and metadata intentionally match, as independently loaded
		// Pine configurations use the same direct-fallback name.
		return corechain.NewNode("direct-fallback", "",
			corechain.TransportNodeOption(transport),
			corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"})))
	}
	broken := &refusingTransport{refused: map[string]error{host: failure}}
	brokenNode := newDirect(broken)
	brokenRouter := newTestRouter(brokenNode)
	if err := dial(t, brokenRouter, host); !errors.Is(err, failure) {
		t.Fatalf("initial direct failure=%v, want %v", err, failure)
	}
	if err := dial(t, brokenRouter, host); !errors.Is(err, pineroute.ErrNoRoute) || broken.connects.Load() != 1 {
		t.Fatalf("same connector lost its refusal cache: err=%v attempts=%d", err, broken.connects.Load())
	}
	// GOST can copy nodes while constructing multiplexed routes. Copies of
	// one connector must keep its identity instead of bypassing the cache.
	if err := dial(t, newTestRouter(brokenNode.Copy()), host); !errors.Is(err, pineroute.ErrNoRoute) {
		t.Fatalf("a node copy bypassed its connector's refusal: %v", err)
	}
	for _, name := range []string{"independent router", "replacement after reload"} {
		t.Run(name, func(t *testing.T) {
			healthy := &refusingTransport{}
			if err := dial(t, newTestRouter(newDirect(healthy)), host); err != nil {
				t.Fatalf("another direct connector inherited the refusal: %v", err)
			}
			if healthy.connects.Load() != 1 {
				t.Fatal("healthy direct connector was not attempted")
			}
		})
	}
}

func TestDirectFailoverTriesIndependentConnectors(t *testing.T) {
	host := fmt.Sprintf("direct-failover-%d.example:443", time.Now().UnixNano())
	failure := errors.New("first connector cannot reach destination")
	broken := &refusingTransport{refused: map[string]error{host: failure}}
	healthy := &refusingTransport{}
	nodes := make([]*corechain.Node, 0, 2)
	for _, transport := range []*refusingTransport{broken, healthy} {
		nodes = append(nodes, corechain.NewNode("direct-fallback", "",
			corechain.TransportNodeOption(transport),
			corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct"}))))
	}
	router := newTestRouter(nodes...)
	if err := dial(t, router, host); err != nil {
		t.Fatalf("independent direct connector was excluded from failover: %v", err)
	}
	if broken.connects.Load() != 1 || healthy.connects.Load() != 1 {
		t.Fatalf("direct attempts=%d/%d, want 1/1", broken.connects.Load(), healthy.connects.Load())
	}
}

func TestProxyTransportFailuresStillCoolAndFailOver(t *testing.T) {
	for _, stage := range []string{"dial", "authentication", "server_reply", "unsupported_command", "connect_timeout"} {
		t.Run(stage, func(t *testing.T) {
			broken := &refusingTransport{}
			switch stage {
			case "dial":
				broken.dialErr = errors.New("proxy dial failed")
			case "authentication":
				broken.handshakeErr = errors.New("proxy authentication failed")
			case "server_reply":
				broken.refused = map[string]error{"target.example": socksReply(1)}
			case "unsupported_command":
				broken.refused = map[string]error{"target.example": socksReply(7)}
			case "connect_timeout":
				broken.refused = map[string]error{"target.example": context.DeadlineExceeded}
			}
			a := pineNode("er_broken_"+stage, broken)
			b := pineNode("er_healthy_"+stage, &refusingTransport{})
			r := newTestRouter(a, b)
			if err := dial(t, r, "target.example:443"); err != nil {
				t.Fatal(err)
			}
			if a.Marker().Count() != 1 {
				t.Fatalf("proxy failure marker=%d, want 1", a.Marker().Count())
			}
		})
	}
}

func TestCanceledRequestDoesNotDamageRouteHealth(t *testing.T) {
	transport := &refusingTransport{cancelConnect: true}
	node := pineNode("er_cancel", transport)
	r := newTestRouter(node, pineNode("er_unused", &refusingTransport{}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	conn, err := r.Dial(ctx, "tcp", "target.example:443")
	if conn != nil {
		conn.Close()
		t.Fatal("canceled request returned a connection")
	}
	if !errors.Is(err, context.DeadlineExceeded) || node.Marker().Count() != 0 {
		t.Fatalf("cancel error=%v marker=%d", err, node.Marker().Count())
	}
}

func TestRouterAttemptDeadlineStillCoolsProxy(t *testing.T) {
	transport := &refusingTransport{cancelConnect: true}
	node := pineNode("er_attempt_deadline", transport)
	healthy := &refusingTransport{}
	r := newTestRouter(node, pineNode("er_after_deadline", healthy))
	r.Options().Timeout = 10 * time.Millisecond
	if err := dial(t, r, "target.example:443"); err != nil {
		t.Fatalf("per-route timeout did not fail over: %v", err)
	}
	if node.Marker().Count() != 1 || healthy.connects.Load() != 1 {
		t.Fatalf("attempt timeout marker=%d, healthy attempts=%d", node.Marker().Count(), healthy.connects.Load())
	}
}

func TestConcurrentBadDestinationCannotDisableHealthyTraffic(t *testing.T) {
	host := fmt.Sprintf("parallel-%d.example", time.Now().UnixNano())
	r := newTestRouter(
		pineNode("er_a_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4)}}),
		pineNode("er_b_"+host, &refusingTransport{refused: map[string]error{host: socksReply(4)}}),
	)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			address := "healthy.example:443"
			if i%2 == 0 {
				address = host + ":443"
			}
			err := dial(t, r, address)
			if (i%2 == 0) != (err != nil) {
				t.Errorf("address=%s error=%v", address, err)
			}
		}(i)
	}
	wg.Wait()
}

// A CONNECT-style error received while reaching an upstream hop is evidence
// about that hop, not a refusal of the browser's destination.
func TestUpstreamReplyDoesNotPoisonDestinationCache(t *testing.T) {
	for _, stage := range []string{"dial", "handshake"} {
		t.Run(stage, func(t *testing.T) {
			host := fmt.Sprintf("upstream-%s-%d.example", stage, time.Now().UnixNano())
			broken := &refusingTransport{}
			if stage == "dial" {
				broken.dialErr = socksReply(4)
			} else {
				broken.handshakeErr = socksReply(4)
			}
			healthy := &refusingTransport{}
			primary := pineNode("er_primary_"+host, broken)
			r := newTestRouter(primary, pineNode("er_backup_"+host, healthy))
			if err := dial(t, r, host+":443"); err != nil {
				t.Fatal(err)
			}
			if primary.Marker().Count() != 1 {
				t.Fatal("upstream failure did not cool the proxy")
			}
			if !pineroute.Ejected(primary) {
				t.Fatal("an upstream failure did not eject the route")
			}
			if pineroute.Skip(pineroute.WithAttempts(context.Background()), primary, "tcp", host+":443") {
				t.Fatal("upstream failure was incorrectly cached against the destination")
			}
		})
	}
}

// everythingTransport answers every CONNECT with err, like a residential exit
// that has lost connectivity but still completes the SOCKS handshake.
type everythingTransport struct {
	refusingTransport
	err error
}

func (t *everythingTransport) Connect(_ context.Context, conn net.Conn, _, _ string) (net.Conn, error) {
	t.connects.Add(1)
	return conn, t.err
}

func (t *everythingTransport) Copy() corechain.Transporter { return t }

// Mirrors pineroute.escalationHosts.
const escalationHosts = 3

// Production pattern 2026-10-04: one Oxylabs session answered reply 4 after
// ~3 s for every host, so each new host on the Computer waited for it first.
func TestRouteFailingReachableHostsIsQuarantined(t *testing.T) {
	for _, reply := range []uint8{3, 4, 5, 6} {
		t.Run(fmt.Sprint(reply), func(t *testing.T) {
			id := fmt.Sprintf("%d-%d", reply, time.Now().UnixNano())
			broken := &everythingTransport{err: socksReply(reply)}
			second := &refusingTransport{}
			primary := pineNode("er_broken_exit_"+id, broken)
			r := newTestRouter(primary, pineNode("er_second_"+id, second), backupNode("er_fallback_"+id, &refusingTransport{}))

			for i := 1; i <= escalationHosts; i++ {
				if err := dial(t, r, fmt.Sprintf("site-%d-%s.example:443", i, id)); err != nil {
					t.Fatalf("host %d did not fail over: %v", i, err)
				}
			}
			if broken.connects.Load() != escalationHosts || primary.Marker().Count() != 0 {
				t.Fatalf("primary connects=%d marker=%d before quarantine", broken.connects.Load(), primary.Marker().Count())
			}
			for i := 0; i < 5; i++ {
				if err := dial(t, r, fmt.Sprintf("next-%d-%s.example:443", i, id)); err != nil {
					t.Fatal(err)
				}
			}
			if broken.connects.Load() != escalationHosts {
				t.Fatal("quarantined route was still tried first for new hosts")
			}
			if second.connects.Load() != escalationHosts+5 {
				t.Fatalf("second primary served %d, want %d", second.connects.Load(), escalationHosts+5)
			}
		})
	}
}

func backupNode(id string, transport corechain.Transporter) *corechain.Node {
	return corechain.NewNode(id, id+".example:1080",
		corechain.TransportNodeOption(transport),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_id": id, "backup": true})))
}

// Production pattern 2026-10-02/03: dead trackers failed on every route. They
// must not be charged to any route, or a page with a few of them would push
// every route of the Computer out at once.
func TestDestinationsNoRouteReachesAreNotCharged(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	dead := map[string]error{}
	for i := 0; i < 6; i++ {
		dead[fmt.Sprintf("dead-%d-%s.example", i, id)] = socksReply(4)
	}
	a := &refusingTransport{refused: dead}
	r := newTestRouter(pineNode("er_primary_dead_"+id, a), pineNode("er_fallback_dead_"+id, &refusingTransport{refused: dead}))
	for host := range dead {
		if err := dial(t, r, host+":443"); err == nil {
			t.Fatalf("dead host %s connected", host)
		}
	}
	if err := dial(t, r, "healthy-"+id+".example:443"); err != nil {
		t.Fatal(err)
	}
	if a.connects.Load() != int32(len(dead))+1 {
		t.Fatal("healthy host did not use the primary")
	}
}

// Provider policy refusals (ads, restricted targets) and address-family errors
// are about the destination and never quarantine a route.
func TestPolicyAndAddressFamilyRefusalsNeverQuarantine(t *testing.T) {
	for _, reply := range []uint8{2, 8} {
		t.Run(fmt.Sprint(reply), func(t *testing.T) {
			id := fmt.Sprintf("%d-%d", reply, time.Now().UnixNano())
			policy := &everythingTransport{err: socksReply(reply)}
			r := newTestRouter(pineNode("er_policy_"+id, policy), pineNode("er_policy_backup_"+id, &refusingTransport{}))
			for i := 0; i < 10; i++ {
				if err := dial(t, r, fmt.Sprintf("ads-%d-%s.example:443", i, id)); err != nil {
					t.Fatal(err)
				}
			}
			if policy.connects.Load() != 10 {
				t.Fatalf("route stopped being tried after reply %d refusals: %d", reply, policy.connects.Load())
			}
		})
	}
}

// Repeated failures for one host are one piece of evidence, not three.
func TestRepeatedHostDoesNotQuarantine(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	host := "flaky-" + id + ".example"
	primary := &refusingTransport{refused: map[string]error{host: socksReply(4)}}
	r := newTestRouter(pineNode("er_one_host_"+id, primary), pineNode("er_one_host_backup_"+id, &refusingTransport{}))
	for _, port := range []string{"443", "8443", "9443", "10443"} {
		if err := dial(t, r, host+":"+port); err != nil {
			t.Fatal(err)
		}
	}
	before := primary.connects.Load()
	if err := dial(t, r, "other-"+id+".example:443"); err != nil {
		t.Fatal(err)
	}
	if primary.connects.Load() != before+1 {
		t.Fatal("one failing host quarantined the whole route")
	}
}

// Routes with complementary reachability charge each other. The cap stops the
// second ejection, and selection falls back to the full set anyway.
func TestComplementaryRoutesNeverBlackOut(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	aRefuses, bRefuses := map[string]error{}, map[string]error{}
	for i := 0; i < 3; i++ {
		aRefuses[fmt.Sprintf("a-%d-%s.example", i, id)] = socksReply(4)
		bRefuses[fmt.Sprintf("b-%d-%s.example", i, id)] = socksReply(4)
	}
	aNode := pineNode("er_comp_a_"+id, &refusingTransport{refused: aRefuses})
	bNode := pineNode("er_comp_b_"+id, &refusingTransport{refused: bRefuses})
	r := newTestRouter(aNode, bNode)
	for _, hosts := range []map[string]error{aRefuses, bRefuses} {
		for host := range hosts {
			if err := dial(t, r, host+":443"); err != nil {
				t.Fatalf("%s: %v", host, err)
			}
		}
	}
	if left := pineroute.WithoutEjected([]*corechain.Node{aNode, bNode}); left == nil || len(left) != 1 {
		t.Fatalf("want exactly one of two routes ejected, %d left", len(left))
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := dial(t, r, fmt.Sprintf("shared-%d-%s.example:443", i, id)); err != nil {
				t.Errorf("quarantine blacked out the Computer: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

// A quarantined route that still works must win over a preferred route that
// is in its failure cooldown, even when that route is the only preferred one.
func TestCooledPreferredRouteDoesNotBeatUsableQuarantinedRoute(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	refused := map[string]error{}
	for i := 0; i < 3; i++ {
		refused[fmt.Sprintf("q-%d-%s.example", i, id)] = socksReply(4)
	}
	quarantined := &refusingTransport{refused: refused}
	cooled := &refusingTransport{}
	r := newTestRouter(pineNode("er_q_"+id, quarantined), pineNode("er_cooled_"+id, cooled))
	for host := range refused {
		if err := dial(t, r, host+":443"); err != nil {
			t.Fatal(err)
		}
	}
	cooled.handshakeErr = errors.New("proxy authentication failed")
	if err := dial(t, r, "first-"+id+".example:443"); err != nil {
		t.Fatalf("quarantined route did not serve after the preferred route failed: %v", err)
	}
	before := cooled.dials.Load()
	for i := 0; i < 3; i++ {
		if err := dial(t, r, fmt.Sprintf("later-%d-%s.example:443", i, id)); err != nil {
			t.Fatal(err)
		}
	}
	if cooled.dials.Load() != before {
		t.Fatal("a route in failure cooldown was dialed ahead of a usable quarantined route")
	}
}

// A lone preferred route is judged by the selector's own cooldown rules: once
// its cooldown has expired, or while it is below maxFails, it is still
// preferred over a quarantined route even though its failure count is non-zero.
func TestLonePreferredRouteFollowsSelectorCooldown(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxFails    int
		failTimeout time.Duration
		wait        time.Duration
	}{
		{"cooldown expired", 1, 20 * time.Millisecond, 40 * time.Millisecond},
		{"below max fails", 3, 30 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := fmt.Sprintf("%d-%d", tc.maxFails, time.Now().UnixNano())
			refused := map[string]error{}
			for i := 0; i < 3; i++ {
				refused[fmt.Sprintf("q-%d-%s.example", i, id)] = socksReply(4)
			}
			quarantined := &refusingTransport{refused: refused}
			preferred := &refusingTransport{}
			preferredNode := pineNode("er_pref_"+id, preferred)
			r := newTestRouterWithFailFilter(tc.maxFails, tc.failTimeout, pineNode("er_quar_"+id, quarantined), preferredNode)
			for host := range refused {
				if err := dial(t, r, host+":443"); err != nil {
					t.Fatal(err)
				}
			}
			preferred.handshakeErr = errors.New("proxy authentication failed")
			if err := dial(t, r, "blip-"+id+".example:443"); err != nil {
				t.Fatal(err)
			}
			preferred.handshakeErr = nil
			if preferredNode.Marker().Count() == 0 {
				t.Fatal("precondition: preferred route should carry a failure")
			}
			time.Sleep(tc.wait)
			quarantinedBefore, preferredBefore := quarantined.connects.Load(), preferred.connects.Load()
			if err := dial(t, r, "next-"+id+".example:443"); err != nil {
				t.Fatal(err)
			}
			if quarantined.connects.Load() != quarantinedBefore || preferred.connects.Load() != preferredBefore+1 {
				t.Fatalf("quarantined=%d preferred=%d: an eligible preferred route lost to quarantine",
					quarantined.connects.Load()-quarantinedBefore, preferred.connects.Load()-preferredBefore)
			}
		})
	}
}

// Quarantine outranks the backup flag: a healthy fallback serves before a
// quarantined primary.
func TestQuarantinedPrimaryYieldsToBackup(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	refused := map[string]error{}
	for i := 0; i < 3; i++ {
		refused[fmt.Sprintf("p-%d-%s.example", i, id)] = socksReply(4)
	}
	primary := &refusingTransport{refused: refused}
	backup := &refusingTransport{}
	r := newTestRouter(pineNode("er_qp_"+id, primary), backupNode("er_qb_"+id, backup))
	for host := range refused {
		if err := dial(t, r, host+":443"); err != nil {
			t.Fatal(err)
		}
	}
	before := primary.connects.Load()
	if err := dial(t, r, "after-"+id+".example:443"); err != nil {
		t.Fatal(err)
	}
	if primary.connects.Load() != before || backup.connects.Load() != 4 {
		t.Fatalf("primary=%d backup=%d after quarantine", primary.connects.Load()-before, backup.connects.Load())
	}
}

// Explicit-country plans have no direct node. A quarantined single route must
// still be used rather than failing the request.
func TestQuarantinedOnlyRouteIsStillUsed(t *testing.T) {
	id := fmt.Sprint(time.Now().UnixNano())
	refused := map[string]error{}
	for i := 0; i < 3; i++ {
		refused[fmt.Sprintf("only-%d-%s.example", i, id)] = socksReply(4)
	}
	only := &refusingTransport{refused: refused}
	other := &refusingTransport{}
	onlyNode := pineNode("er_only_"+id, only)
	r := newTestRouter(onlyNode, pineNode("er_other_"+id, other))
	for host := range refused {
		if err := dial(t, r, host+":443"); err != nil {
			t.Fatal(err)
		}
	}
	solo := newTestRouter(onlyNode)
	if err := dial(t, solo, "solo-"+id+".example:443"); err != nil {
		t.Fatalf("quarantined sole route was not used: %v", err)
	}
}

type eventLog struct {
	mu     sync.Mutex
	events []pineevent.Event
}

func captureEvents(t *testing.T) *eventLog {
	t.Helper()
	log := &eventLog{}
	restore := pineevent.CaptureForTest(func(event pineevent.Event) {
		log.mu.Lock()
		log.events = append(log.events, event)
		log.mu.Unlock()
	})
	t.Cleanup(restore)
	return log
}

func (l *eventLog) kind(kind string) []pineevent.Event {
	return l.match(func(event pineevent.Event) bool { return event.Kind == kind })
}

// Tests filter by their own route or host: timers of earlier tests can still
// emit while a later test captures.
func (l *eventLog) match(keep func(pineevent.Event) bool) []pineevent.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var matched []pineevent.Event
	for _, event := range l.events {
		if keep(event) {
			matched = append(matched, event)
		}
	}
	return matched
}

func (l *eventLog) routeEvents(kind, routeID string) []pineevent.Event {
	return l.match(func(event pineevent.Event) bool { return event.Kind == kind && event.RouteID == routeID })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A dead route is avoided for growing periods and comes back to the base
// period after healthy windows (base scaled down to 200 ms).
func TestDeadRouteIsAvoidedForGrowingPeriods(t *testing.T) {
	base := 200 * time.Millisecond
	defer pineroute.SetTimingForTest(base, time.Second)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	dead := &refusingTransport{dialErr: errors.New("connection refused by proxy")}
	healthy := &refusingTransport{}
	deadNode := pineNode("er_dead_"+id, dead)
	deadNode.Options().Metadata = mdx.NewMetadata(map[string]any{"pine_route_id": "er_dead_" + id, "pine_tier": "primary", "pine_source_list_id": "epl_dead"})
	r := newTestRouterWithFailFilter(1, time.Millisecond, deadNode, pineNode("er_ok_"+id, healthy))
	for round := 1; round <= 3; round++ {
		if err := dial(t, r, fmt.Sprintf("r%d-%s.example:443", round, id)); err != nil {
			t.Fatal(err)
		}
		ejected := events.routeEvents("route_ejected", "er_dead_"+id)
		if len(ejected) != round {
			t.Fatalf("round %d: %d ejections", round, len(ejected))
		}
		got := ejected[round-1]
		want := time.Duration(round) * base
		if got.RouteID != "er_dead_"+id || got.K != round || got.Reason != "connect_error" || got.Tier != "primary" ||
			got.SourceListID != "epl_dead" || got.RouteKind != "managed" ||
			got.TTLMS < want.Milliseconds() || got.TTLMS > (want+want/10).Milliseconds() {
			t.Fatalf("round %d: %+v, want k=%d ttl≈%s", round, got, round, want)
		}
		before := dead.dials.Load()
		if err := dial(t, r, fmt.Sprintf("during-%d-%s.example:443", round, id)); err != nil || dead.dials.Load() != before {
			t.Fatalf("round %d: ejected route was tried (err %v)", round, err)
		}
		waitFor(t, "restore", func() bool { return len(events.routeEvents("route_restored", "er_dead_"+id)) == round })
	}
	// Healthy windows (2 × base each) bring k back down.
	time.Sleep(3 * 2 * base)
	if err := dial(t, r, "after-healthy-"+id+".example:443"); err != nil {
		t.Fatal(err)
	}
	if last := events.routeEvents("route_ejected", "er_dead_"+id); last[len(last)-1].K != 1 {
		t.Fatalf("k=%d after healthy windows, want 1", last[len(last)-1].K)
	}
}

// Never more than half of a hop's routes are ejected, however many are dead.
func TestNoMoreThanHalfTheRoutesAreEjected(t *testing.T) {
	defer pineroute.SetTimingForTest(time.Minute/2, time.Second)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	var nodes []*corechain.Node
	for i := 0; i < 3; i++ {
		nodes = append(nodes, pineNode(fmt.Sprintf("er_dead%d_%s", i, id), &refusingTransport{handshakeErr: errors.New("reset")}))
	}
	healthy := &refusingTransport{}
	nodes = append(nodes, pineNode("er_alive_"+id, healthy))
	r := newTestRouter(nodes...)
	for i := 0; i < 5; i++ {
		if err := dial(t, r, fmt.Sprintf("h%d-%s.example:443", i, id)); err != nil {
			t.Fatal(err)
		}
	}
	ejected := events.match(func(event pineevent.Event) bool {
		return event.Kind == "route_ejected" && strings.HasSuffix(event.RouteID, "_"+id)
	})
	if n := len(ejected); n != 2 {
		t.Fatalf("%d of 4 routes ejected, want 2", n)
	}
	if left := pineroute.WithoutEjected(nodes); len(left) != 2 {
		t.Fatalf("%d routes left, want 2", len(left))
	}
}

// A destination refusing every route is that site's problem: the routes stay.
func TestSingleSiteRefusalNeverEjects(t *testing.T) {
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "one-site-" + id + ".example"
	a := &refusingTransport{refused: map[string]error{host: socksReply(4)}}
	b := &refusingTransport{refused: map[string]error{host: socksReply(5)}}
	r := newTestRouter(pineNode("er_site_a_"+id, a), pineNode("er_site_b_"+id, b))
	for i := 0; i < 50; i++ {
		_ = dial(t, r, fmt.Sprintf("%s:%d", host, 1000+i))
	}
	ejected := events.match(func(event pineevent.Event) bool {
		return event.Kind == "route_ejected" && strings.HasSuffix(event.RouteID, "_"+id)
	})
	if n := len(ejected); n != 0 {
		t.Fatalf("a single-site refusal ejected %d routes", n)
	}
}

// When every route is cooling down, a request still tries one rather than
// failing with no route (explicit plan: no direct node).
func TestEveryRouteCooledStillDials(t *testing.T) {
	defer pineroute.SetTimingForTest(time.Minute/2, time.Second)()
	id := fmt.Sprint(time.Now().UnixNano())
	a := &refusingTransport{handshakeErr: errors.New("reset")}
	b := &refusingTransport{handshakeErr: errors.New("reset")}
	r := newTestRouter(pineNode("er_cool_a_"+id, a), pineNode("er_cool_b_"+id, b))
	if err := dial(t, r, "first-"+id+".example:443"); err == nil {
		t.Fatal("precondition: both routes should fail")
	}
	a.handshakeErr, b.handshakeErr = nil, nil
	if err := dial(t, r, "second-"+id+".example:443"); err != nil {
		t.Fatalf("every route cooled produced %v instead of a panic attempt", err)
	}
}

// A 1,000-request no_route burst for one destination produces a handful of
// request events; the browser still sees every failure.
func TestNoRouteBurstCollapses(t *testing.T) {
	defer pineroute.SetTimingForTest(time.Minute/2, 1500*time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "storm-" + id + ".example"
	r := newTestRouter(
		pineNode("er_storm_a_"+id, &refusingTransport{refused: map[string]error{host: socksReply(2)}}),
		pineNode("er_storm_b_"+id, &refusingTransport{refused: map[string]error{host: socksReply(2)}}))
	if err := dial(t, r, host+":443"); err == nil {
		t.Fatal("precondition: every route refuses")
	}
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := dial(t, r, host+":443"); errors.Is(err, pineroute.ErrNoRoute) {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 1000 {
		t.Fatalf("browser saw %d no_route failures, want 1000", failures.Load())
	}
	stormRequests := func() []pineevent.Event {
		return events.match(func(event pineevent.Event) bool { return event.Kind == "request" && event.DestinationHost == host })
	}
	waitFor(t, "every window's summary", func() bool {
		total := 0
		for _, event := range stormRequests() {
			if event.ErrorClass == "no_route" {
				if event.SuppressedCount == 0 {
					total++
				}
				total += event.SuppressedCount
			}
		}
		return total >= 1000
	})
	var noRoute, leaders, suppressed int
	for _, event := range stormRequests() {
		if event.ErrorClass == "no_route" {
			noRoute++
			if event.SuppressedCount == 0 {
				leaders++
			}
			suppressed += event.SuppressedCount
			if event.Attempts != 0 || event.FailureCause != "vendor_policy" || event.Excluded == nil ||
				*event.Excluded != (pineroute.Excluded{Policy: 2}) {
				t.Fatalf("unexpected no_route event %+v", event)
			}
		}
	}
	if leaders+suppressed != 1000 || noRoute > 4 {
		t.Fatalf("no_route events=%d leaders=%d suppressed=%d for 1000 failures", noRoute, leaders, suppressed)
	}
}

// Only requests whose routes were all excluded by per-destination refusals
// are merged: a matcher or bypass that leaves no route is reported each time.
func TestNoRouteFromOtherExclusionsIsNotMerged(t *testing.T) {
	defer pineroute.SetTimingForTest(time.Minute/2, time.Minute)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	only := pineNode("er_matcher_"+id, &refusingTransport{})
	only.Options().Matcher = neverMatches{}
	r := newTestRouter(only)
	for i := 0; i < 5; i++ {
		if err := dial(t, r, "filtered-"+id+".example:443"); !errors.Is(err, pineroute.ErrNoRoute) {
			t.Fatalf("want no_route, got %v", err)
		}
	}
	filtered := events.match(func(event pineevent.Event) bool {
		return event.Kind == "request" && event.DestinationHost == "filtered-"+id+".example"
	})
	if n := len(filtered); n != 5 {
		t.Fatalf("%d request events for 5 matcher exclusions, want 5", n)
	}
}

type neverMatches struct{}

func (neverMatches) Match(*routing.Request) bool { return false }

// bypassAll excludes every destination, like an ISP node kept for login sites
// when the destination is not one.
type bypassAll struct{}

func (bypassAll) Contains(context.Context, string, string, ...bypass.Option) bool { return true }
func (bypassAll) IsWhitelist() bool                                               { return false }

// A hop with ISP nodes that node bypass excludes for the destination still
// merges the storm when every candidate refused it.
func TestNoRouteStormMergesWithBypassedNodesInTheHop(t *testing.T) {
	defer pineroute.SetTimingForTest(time.Minute/2, time.Minute)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	host := "private-" + id + ".example"
	isp := pineNode("er_isp_"+id, &refusingTransport{})
	isp.Options().Bypass = bypassAll{}
	r := newTestRouter(
		pineNode("er_bp_a_"+id, &refusingTransport{refused: map[string]error{host: socksReply(2)}}),
		pineNode("er_bp_b_"+id, &refusingTransport{refused: map[string]error{host: socksReply(4)}}),
		isp)
	if err := dial(t, r, host+":80"); err == nil {
		t.Fatal("precondition: every managed route refuses")
	}
	for i := 0; i < 20; i++ {
		if err := dial(t, r, host+":80"); !errors.Is(err, pineroute.ErrNoRoute) {
			t.Fatalf("want no_route, got %v", err)
		}
	}
	noRoute := events.match(func(event pineevent.Event) bool {
		return event.Kind == "request" && event.DestinationHost == host && event.ErrorClass == "no_route"
	})
	if len(noRoute) != 1 {
		t.Fatalf("%d no_route events reported at once for 20 failures, want 1 leader", len(noRoute))
	}
	// Every node bypassed is a configuration gap, reported one by one.
	gap := pineNode("er_gap_"+id, &refusingTransport{})
	gap.Options().Bypass = bypassAll{}
	only := newTestRouter(gap)
	for i := 0; i < 3; i++ {
		_ = dial(t, only, "gap-"+id+".example:80")
	}
	if n := len(events.match(func(event pineevent.Event) bool {
		return event.Kind == "request" && event.DestinationHost == "gap-"+id+".example"
	})); n != 3 {
		t.Fatalf("%d events for 3 all-bypassed requests, want 3", n)
	}
}
