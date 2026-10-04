package chain

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	corehop "github.com/go-gost/core/hop"
	"github.com/go-gost/x/internal/pineroute"
	xlogger "github.com/go-gost/x/logger"
	xselector "github.com/go-gost/x/selector"
)

type upstreamReplyError struct{}

func (upstreamReplyError) Error() string          { return "upstream host unreachable" }
func (upstreamReplyError) SOCKS5ReplyCode() uint8 { return 4 }

func TestIntermediateProxyFailureStillMarksChainUnhealthy(t *testing.T) {
	wasEnabled := pineroute.Enabled
	pineroute.Enabled = true
	defer func() { pineroute.Enabled = wasEnabled }()

	first := corechain.NewNode("first", "first.example:1080",
		corechain.TransportNodeOption(&testTransport{connectErr: upstreamReplyError{}}))
	last := corechain.NewNode("last", "last.example:1080", corechain.TransportNodeOption(&testTransport{}))
	chainer := NewChain("multi-hop")
	chainer.AddHop(&testHop{nodes: []*corechain.Node{first}})
	chainer.AddHop(&testHop{nodes: []*corechain.Node{last}})
	ctx := pineroute.WithAttempts(context.Background())
	route := chainer.Route(ctx, "tcp", "destination.example:443")
	conn, err := route.Dial(ctx, "tcp", "destination.example:443", corechain.LoggerDialOption(xlogger.Nop()))
	if conn != nil {
		conn.Close()
		t.Fatal("intermediate failure returned a connection")
	}
	if !errors.Is(err, upstreamReplyError{}) || pineroute.DestinationScoped("tcp", err) {
		t.Fatalf("upstream failure lost its phase or identity: %v", err)
	}
	if chainer.Marker().Count() != 1 || last.Marker().Count() != 1 {
		t.Fatalf("upstream failure did not affect chain health: chain=%d node=%d", chainer.Marker().Count(), last.Marker().Count())
	}
}

func TestRouteMarksFinalNodeWhenConnectFails(t *testing.T) {
	connectErr := errors.New("upstream rejected CONNECT")
	transport := &testTransport{connectErr: connectErr}
	node := corechain.NewNode("proxy", "proxy.example:1080", corechain.TransportNodeOption(transport))
	route := NewRoute()
	route.addNode(node)

	connection, err := route.Dial(context.Background(), "tcp", "target.example:443", corechain.LoggerDialOption(xlogger.Nop()))
	if connection != nil {
		_ = connection.Close()
		t.Fatal("failed CONNECT returned a connection")
	}
	if !errors.Is(err, connectErr) {
		t.Fatalf("Dial error = %v", err)
	}
	if got := node.Marker().Count(); got != 1 {
		t.Fatalf("final node failure count = %d, want 1", got)
	}
}

func TestRouteResetsFinalNodeWhenConnectSucceeds(t *testing.T) {
	transport := &testTransport{}
	node := corechain.NewNode("proxy", "proxy.example:1080", corechain.TransportNodeOption(transport))
	node.Marker().Mark()
	route := NewRoute()
	route.addNode(node)

	connection, err := route.Dial(context.Background(), "tcp", "target.example:443", corechain.LoggerDialOption(xlogger.Nop()))
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if got := node.Marker().Count(); got != 0 {
		t.Fatalf("final node failure count = %d, want 0", got)
	}
}

func TestRoutePreservesFinalNodeFailuresUntilConnectSucceeds(t *testing.T) {
	connectErr := errors.New("upstream rejected CONNECT")
	primaryTransport := &testTransport{connectErr: connectErr}
	primary := corechain.NewNode("primary", "primary.example:1080", corechain.TransportNodeOption(primaryTransport))
	fallback := corechain.NewNode("fallback", "fallback.example:1080", corechain.TransportNodeOption(&testTransport{}))
	hop := &testHop{
		nodes: []*corechain.Node{primary, fallback},
		selector: xselector.NewSelector(
			xselector.FIFOStrategy[*corechain.Node](),
			xselector.FailFilter[*corechain.Node](2, time.Minute),
		),
	}
	chainer := NewChain("proxy")
	chainer.AddHop(hop)

	for attempt := 1; attempt <= 2; attempt++ {
		connection, err := chainer.Route(context.Background(), "tcp", "target.example:443").Dial(
			context.Background(), "tcp", "target.example:443", corechain.LoggerDialOption(xlogger.Nop()),
		)
		if connection != nil {
			_ = connection.Close()
			t.Fatalf("attempt %d returned a connection", attempt)
		}
		if !errors.Is(err, connectErr) {
			t.Fatalf("attempt %d error = %v", attempt, err)
		}
		if got := primary.Marker().Count(); got != int64(attempt) {
			t.Fatalf("attempt %d primary failure count = %d", attempt, got)
		}
	}

	route := chainer.Route(context.Background(), "tcp", "target.example:443")
	if got := route.Nodes()[0]; got != fallback {
		t.Fatalf("selected node = %q, want fallback", got.Name)
	}
	connection, err := route.Dial(context.Background(), "tcp", "target.example:443", corechain.LoggerDialOption(xlogger.Nop()))
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
}

func TestRoutePreservesChainFailuresUntilFailover(t *testing.T) {
	connectErr := errors.New("upstream rejected CONNECT")
	primaryNode := corechain.NewNode("primary", "primary.example:1080", corechain.TransportNodeOption(&testTransport{connectErr: connectErr}))
	fallbackNode := corechain.NewNode("fallback", "fallback.example:1080", corechain.TransportNodeOption(&testTransport{}))
	primary := NewChain("primary")
	primary.AddHop(&testHop{nodes: []*corechain.Node{primaryNode}})
	fallback := NewChain("fallback")
	fallback.AddHop(&testHop{nodes: []*corechain.Node{fallbackNode}})
	group := NewChainGroup(primary, fallback).WithSelector(xselector.NewSelector(
		xselector.FIFOStrategy[corechain.Chainer](),
		xselector.FailFilter[corechain.Chainer](2, time.Minute),
	))

	for attempt := 1; attempt <= 2; attempt++ {
		route := group.Route(context.Background(), "tcp", "target.example:443")
		connection, err := route.Dial(context.Background(), "tcp", "target.example:443", corechain.LoggerDialOption(xlogger.Nop()))
		if connection != nil {
			_ = connection.Close()
			t.Fatalf("attempt %d returned a connection", attempt)
		}
		if !errors.Is(err, connectErr) {
			t.Fatalf("attempt %d Dial error = %v", attempt, err)
		}
		if got := primary.Marker().Count(); got != int64(attempt) {
			t.Fatalf("attempt %d primary chain failure count = %d", attempt, got)
		}
	}

	route := group.Route(context.Background(), "tcp", "target.example:443")
	if got := route.Nodes()[0]; got != fallbackNode {
		t.Fatalf("selected node = %q, want fallback chain", got.Name)
	}
}

type testHop struct {
	nodes    []*corechain.Node
	selector interface {
		Select(context.Context, ...*corechain.Node) *corechain.Node
	}
}

func (h *testHop) Select(ctx context.Context, _ ...corehop.SelectOption) *corechain.Node {
	if h.selector != nil {
		return h.selector.Select(ctx, h.nodes...)
	}
	if len(h.nodes) == 0 {
		return nil
	}
	return h.nodes[0]
}

type testTransport struct {
	connectErr error
	options    corechain.TransportOptions
}

func (t *testTransport) Dial(context.Context, string) (net.Conn, error) {
	client, peer := net.Pipe()
	go func() {
		defer peer.Close()
		_, _ = peer.Read(make([]byte, 1))
	}()
	return client, nil
}

func (t *testTransport) Handshake(_ context.Context, connection net.Conn) (net.Conn, error) {
	return connection, nil
}

func (t *testTransport) Connect(_ context.Context, connection net.Conn, _, _ string) (net.Conn, error) {
	return connection, t.connectErr
}

func (t *testTransport) Bind(context.Context, net.Conn, string, string, ...connector.BindOption) (net.Listener, error) {
	return nil, connector.ErrBindUnsupported
}

func (t *testTransport) Multiplex() bool { return false }

func (t *testTransport) Options() *corechain.TransportOptions { return &t.options }

func (t *testTransport) Copy() corechain.Transporter { return t }
