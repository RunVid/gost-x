package chain

import (
	"context"
	"errors"
	"net"
	"testing"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	xlogger "github.com/go-gost/x/logger"
)

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
