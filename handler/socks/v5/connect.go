package v5

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-gost/core/bypass"
	"github.com/go-gost/core/limiter"
	"github.com/go-gost/core/logger"
	"github.com/go-gost/core/observer/stats"
	"github.com/go-gost/gosocks5"
	xctx "github.com/go-gost/x/ctx"
	ictx "github.com/go-gost/x/internal/ctx"
	xio "github.com/go-gost/x/internal/io"
	xnet "github.com/go-gost/x/internal/net"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/util/sniffing"
	traffic_wrapper "github.com/go-gost/x/limiter/traffic/wrapper"
	stats_wrapper "github.com/go-gost/x/observer/stats/wrapper"
	xrecorder "github.com/go-gost/x/recorder"
)

func (h *socks5Handler) handleConnect(ctx context.Context, conn net.Conn, network, address string, ro *xrecorder.HandlerRecorderObject, log logger.Logger) error {
	log = log.WithFields(map[string]any{
		"dst":  fmt.Sprintf("%s/%s", address, network),
		"cmd":  "connect",
		"host": address,
	})
	log.Debugf("%s >> %s", conn.RemoteAddr(), address)

	{
		clientID := xctx.ClientIDFromContext(ctx)
		rw := traffic_wrapper.WrapReadWriter(
			h.limiter,
			conn,
			string(clientID),
			limiter.ServiceOption(h.options.Service),
			limiter.ScopeOption(limiter.ScopeClient),
			limiter.NetworkOption(network),
			limiter.AddrOption(address),
			limiter.ClientOption(string(clientID)),
			limiter.SrcOption(conn.RemoteAddr().String()),
		)
		if h.options.Observer != nil {
			pstats := h.stats.Stats(string(clientID))
			pstats.Add(stats.KindTotalConns, 1)
			pstats.Add(stats.KindCurrentConns, 1)
			defer pstats.Add(stats.KindCurrentConns, -1)
			rw = stats_wrapper.WrapReadWriter(rw, pstats)
		}

		conn = xnet.NewReadWriteConn(rw, rw, conn)
	}

	if h.options.Bypass != nil && h.options.Bypass.Contains(ctx, network, address, bypass.WithService(h.options.Service)) {
		resp := gosocks5.NewReply(gosocks5.NotAllowed, nil)
		log.Trace(resp)
		log.Debug("bypass: ", address)
		return resp.Write(conn)
	}

	switch h.md.hash {
	case "host":
		ctx = xctx.ContextWithHash(ctx, &xctx.Hash{Source: address})
	}

	var buf bytes.Buffer
	ctx, dialState := pineevent.ContextWithDialState(ctx)
	cc, err := h.options.Router.Dial(ictx.ContextWithBuffer(ctx, &buf), network, address)
	ro.Route = buf.String()
	if err != nil {
		resp := gosocks5.NewReply(gosocks5.NetUnreachable, nil)
		log.Trace(resp)
		resp.Write(conn)
		return err
	}
	defer cc.Close()

	log = log.WithFields(map[string]any{"src": cc.LocalAddr().String(), "dst": cc.RemoteAddr().String()})
	ro.SrcAddr = cc.LocalAddr().String()
	ro.DstAddr = cc.RemoteAddr().String()

	resp := gosocks5.NewReply(gosocks5.Succeeded, nil)
	log.Trace(resp)
	if err := resp.Write(conn); err != nil {
		log.Error(err)
		return err
	}

	if h.md.sniffing {
		if h.md.sniffingTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(h.md.sniffingTimeout))
		}

		br := bufio.NewReader(conn)
		proto, _ := sniffing.Sniff(ctx, br)
		ro.Proto = proto

		if h.md.sniffingTimeout > 0 {
			conn.SetReadDeadline(time.Time{})
		}

		dial := func(ctx context.Context, network, address string) (net.Conn, error) {
			return cc, nil
		}
		dialTLS := func(ctx context.Context, network, address string, cfg *tls.Config) (net.Conn, error) {
			return cc, nil
		}
		sniffer := &sniffing.Sniffer{
			Websocket:           h.md.sniffingWebsocket,
			WebsocketSampleRate: h.md.sniffingWebsocketSampleRate,
			Recorder:            h.recorder.Recorder,
			RecorderOptions:     h.recorder.Options,
			Certificate:         h.md.certificate,
			PrivateKey:          h.md.privateKey,
			NegotiatedProtocol:  h.md.alpn,
			CertPool:            h.certPool,
			MitmBypass:          h.md.mitmBypass,
			ReadTimeout:         h.md.readTimeout,
		}

		conn = xnet.NewReadWriteConn(br, conn, conn)
		switch proto {
		case sniffing.ProtoHTTP:
			return sniffer.HandleHTTP(ctx, "tcp", conn,
				sniffing.WithService(h.options.Service),
				sniffing.WithDial(dial),
				sniffing.WithDialTLS(dialTLS),
				sniffing.WithRecorderObject(ro),
				sniffing.WithLog(log),
			)
		case sniffing.ProtoTLS:
			return sniffer.HandleTLS(ctx, "tcp", conn,
				sniffing.WithService(h.options.Service),
				sniffing.WithDial(dial),
				sniffing.WithDialTLS(dialTLS),
				sniffing.WithRecorderObject(ro),
				sniffing.WithLog(log),
			)
		}
	}

	t := time.Now()
	log.Infof("%s <-> %s", conn.RemoteAddr(), address)
	// xnet.Transport(conn, cc)
	clientConn := &countingConn{Conn: conn, startedAt: t}
	upstreamConn := &countingConn{Conn: cc, startedAt: t}
	xnet.Pipe(ctx, clientConn, upstreamConn)
	route, outcome := dialState.SelectedRoute()
	destinationHost, destinationPort := pineevent.Destination(address)
	pineevent.Emit(pineevent.Event{
		Kind:                  "relay",
		ConnectionID:          xctx.SidFromContext(ctx).String(),
		Network:               network,
		DestinationHost:       destinationHost,
		DestinationPort:       destinationPort,
		RouteID:               route.RouteID,
		SourceListID:          route.SourceListID,
		Tier:                  route.Tier,
		RouteKind:             route.Kind,
		Outcome:               outcome,
		DurationMS:            time.Since(t).Milliseconds(),
		FirstDownstreamByteMS: clientConn.FirstWriteMS(),
		BytesUp:               upstreamConn.BytesWritten(),
		BytesDown:             clientConn.BytesWritten(),
		UploadActiveMS:        upstreamConn.ActiveWriteMS(),
		DownloadActiveMS:      clientConn.ActiveWriteMS(),
	})
	log.WithFields(map[string]any{
		"duration": time.Since(t),
	}).Infof("%s >-< %s", conn.RemoteAddr(), address)

	return nil
}

type countingConn struct {
	net.Conn
	bytesWritten atomic.Int64
	firstWriteNS atomic.Int64
	lastWriteNS  atomic.Int64
	startedAt    time.Time
}

func (c *countingConn) Write(payload []byte) (int, error) {
	started := time.Since(c.startedAt).Nanoseconds() + 1
	n, err := c.Conn.Write(payload)
	if n > 0 {
		// Store nanoseconds + 1 so an immediate first byte is distinguishable
		// from a connection that never delivered downstream data.
		c.firstWriteNS.CompareAndSwap(0, started)
		c.lastWriteNS.Store(time.Since(c.startedAt).Nanoseconds() + 1)
	}
	c.bytesWritten.Add(int64(n))
	return n, err
}

func (c *countingConn) ActiveWriteMS() int64 {
	first := c.firstWriteNS.Load()
	last := c.lastWriteNS.Load()
	if first == 0 || last < first {
		return 0
	}
	return (last - first) / int64(time.Millisecond)
}

func (c *countingConn) FirstWriteMS() int64 {
	value := c.firstWriteNS.Load()
	if value == 0 {
		return -1
	}
	return (value - 1) / int64(time.Millisecond)
}

func (c *countingConn) BytesWritten() int64 {
	return c.bytesWritten.Load()
}

func (c *countingConn) CloseRead() error {
	if connection, ok := c.Conn.(xio.CloseRead); ok {
		return connection.CloseRead()
	}
	return xio.ErrUnsupported
}

func (c *countingConn) CloseWrite() error {
	if connection, ok := c.Conn.(xio.CloseWrite); ok {
		return connection.CloseWrite()
	}
	return xio.ErrUnsupported
}
