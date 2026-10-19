package v5

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/go-gost/core/connector"
	"github.com/go-gost/gosocks5"
	xlogger "github.com/go-gost/x/logger"
)

const serverFirstBanner = "SERVER-BANNER"

// fakeSocks5Upstream accepts one client, then answers CONNECT by writing the
// success reply and a server-first banner in a single write so they coalesce
// on the wire. Whatever the client sends next is echoed back.
func fakeSocks5Upstream(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		var greeting [2]byte
		if _, err := io.ReadFull(conn, greeting[:]); err != nil {
			return
		}
		methods := make([]byte, greeting[1])
		if _, err := io.ReadFull(conn, methods); err != nil {
			return
		}
		if _, err := conn.Write([]byte{gosocks5.Ver5, gosocks5.MethodNoAuth}); err != nil {
			return
		}

		req, err := gosocks5.ReadRequest(conn)
		if err != nil || req.Cmd != gosocks5.CmdConnect {
			return
		}

		reply := buildReply(gosocks5.Succeeded, "0.0.0.0", "0")
		if _, err := conn.Write(append(reply, serverFirstBanner...)); err != nil {
			return
		}

		io.Copy(io.Discard, conn)
	}()

	return ln
}

func TestConnectorPreservesServerFirstPayload(t *testing.T) {
	upstream := fakeSocks5Upstream(t)
	auth := url.User("user:pass")

	c := NewConnector(connector.AuthOption(auth), connector.LoggerOption(xlogger.Nop()))
	if err := c.Init(nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := net.Dial("tcp", upstream.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cc, err := c.(connector.Handshaker).Handshake(ctx, conn)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	cc, err = c.Connect(ctx, cc, "tcp", "target.example.com:80")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cc.SetDeadline(time.Now().Add(3 * time.Second))

	banner := make([]byte, len(serverFirstBanner))
	if _, err := io.ReadFull(cc, banner); err != nil {
		t.Fatalf("read server-first payload: %v", err)
	}
	if !bytes.Equal(banner, []byte(serverFirstBanner)) {
		t.Fatalf("banner = %q, want %q", banner, serverFirstBanner)
	}

}
