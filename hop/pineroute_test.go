package hop

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	refused  map[string]error
	connects atomic.Int32
	options  corechain.TransportOptions
}

func (t *refusingTransport) Dial(context.Context, string) (net.Conn, error) {
	client, peer := net.Pipe()
	go func() {
		defer peer.Close()
		_, _ = peer.Read(make([]byte, 1))
	}()
	return client, nil
}

func (t *refusingTransport) Handshake(_ context.Context, conn net.Conn) (net.Conn, error) {
	return conn, nil
}

func (t *refusingTransport) Connect(_ context.Context, conn net.Conn, _, address string) (net.Conn, error) {
	t.connects.Add(1)
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
