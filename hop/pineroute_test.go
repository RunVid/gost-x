package hop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
	mdx "github.com/go-gost/x/metadata"
	xselector "github.com/go-gost/x/selector"
)

func TestMain(m *testing.M) {
	pineroute.Enabled = true
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
	connects      atomic.Int32
	options       corechain.TransportOptions
}

func (t *refusingTransport) Dial(context.Context, string) (net.Conn, error) {
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
	h := NewHop(NodeOption(nodes...), SelectorOption(xselector.NewSelector(
		xselector.FIFOStrategy[*corechain.Node](),
		xselector.FailFilter[*corechain.Node](1, 30*time.Second),
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
			// Simulate the proxy recovering after its route cooldown. There
			// must be no independent destination refusal left behind.
			broken.dialErr, broken.handshakeErr = nil, nil
			primary.Marker().Reset()
			if err := dial(t, r, host+":443"); err != nil {
				t.Fatal(err)
			}
			if broken.connects.Load() != 1 || healthy.connects.Load() != 1 {
				t.Fatal("upstream failure was incorrectly cached against the destination")
			}
		})
	}
}
