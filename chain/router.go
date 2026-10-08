package chain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-gost/core/chain"
	"github.com/go-gost/core/logger"
	"github.com/go-gost/core/recorder"
	xctx "github.com/go-gost/x/ctx"
	ictx "github.com/go-gost/x/internal/ctx"
	xnet "github.com/go-gost/x/internal/net"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
)

type Router struct {
	options chain.RouterOptions
}

func NewRouter(opts ...chain.RouterOption) *Router {
	r := &Router{}
	for _, opt := range opts {
		if opt != nil {
			opt(&r.options)
		}
	}
	if r.options.Timeout == 0 {
		r.options.Timeout = 15 * time.Second
	}

	if r.options.Logger == nil {
		r.options.Logger = logger.Default().WithFields(map[string]any{"kind": "router"})
	}
	return r
}

func (r *Router) Options() *chain.RouterOptions {
	if r == nil {
		return nil
	}
	return &r.options
}

func (r *Router) Dial(ctx context.Context, network, address string) (conn net.Conn, err error) {
	host := address
	if h, _, _ := net.SplitHostPort(address); h != "" {
		host = h
	}
	r.record(ctx, recorder.RecorderServiceRouterDialAddress, []byte(host))

	log := r.options.Logger.WithFields(map[string]any{
		"sid": xctx.SidFromContext(ctx),
	})

	conn, err = r.dial(ctx, network, address, log)
	if err != nil {
		r.record(ctx, recorder.RecorderServiceRouterDialAddressError, []byte(host))
		return
	}

	if network == "udp" || network == "udp4" || network == "udp6" {
		if _, ok := conn.(net.PacketConn); !ok {
			return &packetConn{conn}, nil
		}
	}
	return
}

func (r *Router) record(ctx context.Context, name string, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	for _, rec := range r.options.Recorders {
		if rec.Record == name {
			return rec.Recorder.Record(ctx, data)
		}
	}
	return nil
}

func (r *Router) dial(ctx context.Context, network, address string, log logger.Logger) (conn net.Conn, err error) {
	startedAt := time.Now()
	destinationHost, destinationPort := pineevent.Destination(address)
	count := r.options.Retries + 1
	if count <= 0 {
		count = 1
	}

	log.Debugf("dial %s/%s", address, network)

	attempts := 0
	anyFailed, hedged := false, false
	hedgeResult, raceFirst := "", 0
	var lastAttemptErr error
	selectedRoute := pineevent.Route{Tier: "unselected", Kind: "unselected"}
	ctx = pineroute.WithAttempts(ctx)
	hedgeDelay := pineroute.HedgeDelay()

	// Attempt goroutines only dial. Everything else (route state, evidence,
	// events) happens here, one completed attempt at a time.
	results := make(chan *dialAttempt, 2)
	var inflight []*dialAttempt
	finish := func(a *dialAttempt) {
		attempts := inflight[:0]
		for _, other := range inflight {
			if other != a {
				attempts = append(attempts, other)
			}
		}
		inflight = attempts
	}
	for {
		if len(inflight) == 0 {
			if attempts >= count {
				break
			}
			if pineroute.Enabled && ctx.Err() != nil {
				err = ctx.Err()
				break
			}
			a, launchErr := r.launch(ctx, network, address, attempts+1, false, results, log)
			if a == nil {
				if launchErr == pineroute.ErrNoRoute && lastAttemptErr != nil {
					launchErr = lastAttemptErr
				}
				err = launchErr
				break
			}
			attempts++
			inflight = append(inflight, a)
		}

		var hedgeTimer *time.Timer
		var hedgeFire <-chan time.Time
		if hedgeDelay > 0 && !hedged && len(inflight) == 1 && attempts < count {
			hedgeTimer = time.NewTimer(hedgeDelay - time.Since(inflight[0].startedAt))
			hedgeFire = hedgeTimer.C
		}
		select {
		case a := <-results:
			if hedgeTimer != nil {
				hedgeTimer.Stop()
			}
			finish(a)
			selectedRoute = a.selected
			if a.err == nil {
				// The other attempt, if any, lost: cancel it and close a
				// connection it may still produce. SOCKS connectors bound
				// their exchange by their own timeout (Pine renders 5 s), so
				// a loser that ignores cancellation still ends. A first route
				// still pending when the hedge won is evidence that it is
				// slow; the connector's health marker still sees the loser's
				// real outcome if it arrives.
				for _, loser := range inflight {
					if a.hedge && !loser.hedge {
						pineroute.NoteSlowSuspect(ctx, loser.node, network, address, loser.ipAddr)
					}
					loser.cancel()
					go discard(results)
				}
				inflight = nil
				if hedged && hedgeResult == "" {
					if a.index == raceFirst {
						hedgeResult = "lost"
					}
					if a.hedge {
						hedgeResult = "won"
						if !anyFailed {
							pineroute.MarkHedgeWon(ctx)
						}
					}
				}
				conn, err = a.conn, nil
				if buf := ictx.BufferFromContext(ctx); buf != nil && a.hedge {
					// The caller's record describes the route that served.
					buf.Reset()
					buf.WriteString(a.path)
				}
				r.settle(ctx, a, network, address, destinationHost, destinationPort)
				a.cancel()
				return r.finishDial(ctx, conn, err, startedAt, destinationHost, destinationPort, network, address,
					attempts, anyFailed, hedgeResult, selectedRoute)
			}
			anyFailed = true
			lastAttemptErr = a.err
			err = a.err
			r.settle(ctx, a, network, address, destinationHost, destinationPort)
			log.Errorf("route(retry=%d) %s", a.index-1, a.err)
			if hedged && hedgeResult == "" && len(inflight) == 0 {
				hedgeResult = "failed"
			}
		case <-hedgeFire:
			hedged = true
			raceFirst = inflight[0].index
			h, _ := r.launch(ctx, network, address, attempts+1, true, results, log)
			if h != nil {
				attempts++
				inflight = append(inflight, h)
			} else {
				hedged = false
				hedgeDelay = 0
			}
		}
	}
	return r.finishDial(ctx, conn, err, startedAt, destinationHost, destinationPort, network, address,
		attempts, anyFailed, hedgeResult, selectedRoute)
}

// dialAttempt is one route attempt of a request.
type dialAttempt struct {
	index     int
	hedge     bool
	route     chain.Route
	node      *chain.Node
	selected  pineevent.Route
	ipAddr    string
	path      string
	startedAt time.Time
	cancel    context.CancelFunc
	conn      net.Conn
	err       error
}

// discard receives one late attempt result and closes its connection.
func discard(results <-chan *dialAttempt) {
	if a := <-results; a != nil && a.conn != nil {
		_ = a.conn.Close()
	}
}

// launch selects the next route and starts dialing it. It returns nil and
// the error that ends the request when no attempt can start. A hedge never
// uses the explicit direct node: direct is for exhaustion. Pine configures no
// resolver (the provider resolves), so preparing a hedge does not block on
// DNS while the first attempt may already have answered.
func (r *Router) launch(ctx context.Context, network, address string, index int, hedge bool,
	results chan<- *dialAttempt, log logger.Logger) (*dialAttempt, error) {
	attemptCtx, cancel := ctx, context.CancelFunc(func() {})
	if r.options.Timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, r.options.Timeout)
	} else {
		attemptCtx, cancel = context.WithCancel(ctx)
	}

	buf := ictx.BufferFromContext(ctx)
	if buf != nil && !hedge {
		buf.Reset()
	}
	ipAddr, err := xnet.Resolve(attemptCtx, "ip", address, r.options.Resolver, r.options.HostMapper, log)
	if err != nil {
		cancel()
		log.Error(err)
		return nil, err
	}
	if buf != nil && !hedge {
		buf.Reset()
	}

	var route chain.Route
	if r.options.Chain != nil {
		route = r.options.Chain.Route(attemptCtx, network, ipAddr, chain.WithHostRouteOption(address))
	}
	if buf == nil || hedge {
		buf = &bytes.Buffer{}
	}
	for _, node := range routePath(route) {
		fmt.Fprintf(buf, "%s@%s > ", node.Name, node.Addr)
	}
	fmt.Fprintf(buf, "%s", ipAddr)
	log.Debugf("route(retry=%d) %s", index-1, buf.String())

	// Pine chains list every allowed exit, including an explicit direct
	// node in automatic mode. An empty selection means every route was
	// excluded, so fail instead of falling through to GOST's implicit
	// direct route.
	if pineroute.Enabled && r.options.Chain != nil && (route == nil || len(route.Nodes()) == 0) {
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, pineroute.ErrNoRoute
	}
	if route == nil {
		route = DefaultRoute
	}
	var node *chain.Node
	if path := routePath(route); len(path) > 0 {
		node = path[len(path)-1]
	}
	if hedge && pineroute.IsDirect(node) {
		cancel()
		return nil, nil
	}
	// Mark at launch so a hedge cannot select the route still dialing.
	pineroute.MarkTried(ctx, node)
	a := &dialAttempt{index: index, hedge: hedge, route: route, node: node, selected: pineRoute(route),
		ipAddr: ipAddr, path: buf.String(), startedAt: time.Now(), cancel: cancel}
	go func() {
		a.conn, a.err = route.Dial(attemptCtx, network, ipAddr,
			chain.InterfaceDialOption(r.options.IfceName),
			chain.NetnsDialOption(r.options.Netns),
			chain.SockOptsDialOption(r.options.SockOpts),
			chain.LoggerDialOption(log),
		)
		results <- a
	}()
	return a, nil
}

// settle records a completed attempt: route state, evidence and its event.
func (r *Router) settle(ctx context.Context, a *dialAttempt, network, address, destinationHost string, destinationPort int) {
	err := a.err
	if a.node != nil {
		node := a.node
		pineroute.RecordAttempt(ctx, node, network, a.ipAddr, err)
		pineroute.RecordRefusal(ctx, node, network, address, err)
		pineroute.RecordRouteFailure(ctx, node, network, err)
		if err == nil {
			pineroute.RecordSuccess(ctx, node, network, address)
			pineroute.BlameSuspects(ctx, node, network, a.ipAddr)
		} else {
			pineroute.NoteSuspect(ctx, node, network, address, a.ipAddr, err)
		}
	}
	if err != nil {
		a.cancel()
	}
	result := "success"
	errorClass, socks5Reply := pineevent.ErrorDetails(err)
	if err != nil {
		result = "failure"
	}
	pineevent.Emit(pineevent.Event{
		Kind:            "attempt",
		ConnectionID:    xctx.SidFromContext(ctx).String(),
		Network:         network,
		DestinationHost: destinationHost,
		DestinationPort: destinationPort,
		RouteID:         a.selected.RouteID,
		SourceListID:    a.selected.SourceListID,
		Tier:            a.selected.Tier,
		RouteKind:       a.selected.Kind,
		Attempt:         a.index,
		Result:          result,
		ErrorClass:      errorClass,
		SOCKS5Reply:     socks5Reply,
		DurationMS:      time.Since(a.startedAt).Milliseconds(),
	})
}

// finishDial reports the request.
func (r *Router) finishDial(ctx context.Context, conn net.Conn, err error, startedAt time.Time,
	destinationHost string, destinationPort int, network, address string,
	attempts int, anyFailed bool, hedgeResult string, selectedRoute pineevent.Route) (_ net.Conn, _ error) {
	outcome := "failed"
	if err == nil {
		switch {
		case selectedRoute.Kind == "direct":
			outcome = "direct_fallback_success"
		case anyFailed:
			outcome = "managed_failover_success"
		default:
			outcome = "success"
		}
		pineevent.SetSelectedRoute(ctx, selectedRoute, outcome)
	}
	errorClass, socks5Reply := pineevent.ErrorDetails(err)
	event := pineevent.Event{
		Kind:            "request",
		ConnectionID:    xctx.SidFromContext(ctx).String(),
		Network:         network,
		DestinationHost: destinationHost,
		DestinationPort: destinationPort,
		RouteID:         selectedRoute.RouteID,
		SourceListID:    selectedRoute.SourceListID,
		Tier:            selectedRoute.Tier,
		RouteKind:       selectedRoute.Kind,
		Attempts:        attempts,
		Outcome:         outcome,
		ErrorClass:      errorClass,
		SOCKS5Reply:     socks5Reply,
		FailureCause:    pineroute.FailureCause(ctx, err),
		DurationMS:      time.Since(startedAt).Milliseconds(),
		Hedge:           hedgeResult,
	}
	if attempts == 0 && errors.Is(err, pineroute.ErrNoRoute) && pineroute.AllRefused(ctx) {
		// Every route was excluded for this destination before any attempt.
		// Report the first such failure in a window; the window's summary
		// carries the rest as suppressed_count.
		event.ObservedAtUnixMS = time.Now().UnixMilli()
		if !pineroute.MergeNoRoute(network, address, event, emitNoRouteSummary) {
			return conn, err
		}
	}
	pineevent.Emit(event)
	return conn, err
}

// emitNoRouteSummary reports the no-route failures a window suppressed as
// one request event when the window closes: the latest one's identity, with
// the count.
func emitNoRouteSummary(last any, suppressed int) {
	event, ok := last.(pineevent.Event)
	if !ok {
		return
	}
	event.SuppressedCount = suppressed
	event.ObservedAtUnixMS = time.Now().UnixMilli()
	pineevent.Emit(event)
}

func pineRoute(route chain.Route) pineevent.Route {
	path := routePath(route)
	if len(path) == 0 {
		return pineevent.Route{Tier: "direct", Kind: "direct"}
	}
	node := path[len(path)-1]
	md := node.Options().Metadata
	if md == nil {
		return pineevent.Route{Tier: "unselected", Kind: "unselected"}
	}
	stringValue := func(key string) string {
		value, _ := md.Get(key).(string)
		return value
	}
	return pineevent.Route{
		RouteID:      stringValue("pine_route_id"),
		SourceListID: stringValue("pine_source_list_id"),
		Tier:         stringValue("pine_tier"),
		Kind:         stringValue("pine_route_kind"),
	}
}

func (r *Router) Bind(ctx context.Context, network, address string, opts ...chain.BindOption) (ln net.Listener, err error) {
	count := r.options.Retries + 1
	if count <= 0 {
		count = 1
	}

	log := r.options.Logger.WithFields(map[string]any{
		"sid": xctx.SidFromContext(ctx),
	})

	log.Debugf("bind on %s/%s", address, network)

	for i := 0; i < count; i++ {
		ctx := ctx
		if r.options.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, r.options.Timeout)
			defer cancel()
		}

		var route chain.Route
		if r.options.Chain != nil {
			route = r.options.Chain.Route(ctx, network, address)
			if route == nil || len(route.Nodes()) == 0 {
				err = ErrEmptyRoute
				return
			}
		}

		if log.IsLevelEnabled(logger.DebugLevel) {
			buf := bytes.Buffer{}
			for _, node := range routePath(route) {
				fmt.Fprintf(&buf, "%s@%s > ", node.Name, node.Addr)
			}
			fmt.Fprintf(&buf, "%s", address)
			log.Debugf("route(retry=%d) %s", i, buf.String())
		}

		if route == nil {
			route = DefaultRoute
		}
		ln, err = route.Bind(ctx, network, address, opts...)
		if err == nil {
			break
		}
		log.Errorf("route(retry=%d) %s", i, err)
	}

	return
}

func routePath(route chain.Route) (path []*chain.Node) {
	if route == nil {
		return
	}
	for _, node := range route.Nodes() {
		if tr := node.Options().Transport; tr != nil {
			path = append(path, routePath(tr.Options().Route)...)
		}
		path = append(path, node)
	}
	return
}

type packetConn struct {
	net.Conn
}

func (c *packetConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, err = c.Read(b)
	addr = c.Conn.RemoteAddr()
	return
}

func (c *packetConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	return c.Write(b)
}
