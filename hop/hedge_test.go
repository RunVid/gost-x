package hop

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	xchain "github.com/go-gost/x/chain"
	"github.com/go-gost/x/internal/pineevent"
	"github.com/go-gost/x/internal/pineroute"
	mdx "github.com/go-gost/x/metadata"
)

// slowTransport answers CONNECT after delay with err (nil = connected). With
// ignoreCancel it finishes even after its context is cancelled, like a
// connector that does not watch the context.
type slowTransport struct {
	delay        time.Duration
	err          error
	ignoreCancel bool
	connects     atomic.Int32
	dials        atomic.Int32
	firstDial    atomic.Int64 // unix nanos of the first dial
	canceled     atomic.Int32
	closed       atomic.Int32
	options      corechain.TransportOptions
}

type closeCountingConn struct {
	net.Conn
	closed *atomic.Int32
	once   sync.Once
}

func (c *closeCountingConn) Close() error {
	c.once.Do(func() { c.closed.Add(1) })
	return c.Conn.Close()
}

func (t *slowTransport) Dial(context.Context, string) (net.Conn, error) {
	t.firstDial.CompareAndSwap(0, time.Now().UnixNano())
	t.dials.Add(1)
	client, peer := net.Pipe()
	go func() {
		defer peer.Close()
		_, _ = peer.Read(make([]byte, 1))
	}()
	return &closeCountingConn{Conn: client, closed: &t.closed}, nil
}

func (t *slowTransport) Handshake(_ context.Context, conn net.Conn) (net.Conn, error) {
	return conn, nil
}

func (t *slowTransport) Connect(ctx context.Context, conn net.Conn, _, _ string) (net.Conn, error) {
	t.connects.Add(1)
	select {
	case <-time.After(t.delay):
	case <-ctx.Done():
		t.canceled.Add(1)
		if !t.ignoreCancel {
			return conn, ctx.Err()
		}
		time.Sleep(t.delay)
	}
	return conn, t.err
}

func (t *slowTransport) Bind(context.Context, net.Conn, string, string, ...connector.BindOption) (net.Listener, error) {
	return nil, connector.ErrBindUnsupported
}
func (t *slowTransport) Multiplex() bool                      { return false }
func (t *slowTransport) Options() *corechain.TransportOptions { return &t.options }
func (t *slowTransport) Copy() corechain.Transporter          { return t }

func timedDial(t *testing.T, r interface {
	Dial(context.Context, string, string) (net.Conn, error)
}, address string) (time.Duration, error) {
	t.Helper()
	started := time.Now()
	conn, err := r.Dial(context.Background(), "tcp", address)
	if conn != nil {
		_ = conn.Close()
	}
	return time.Since(started), err
}

func requestEvent(t *testing.T, events *eventLog, host string) pineevent.Event {
	t.Helper()
	var found []pineevent.Event
	waitFor(t, "request event", func() bool {
		found = events.match(func(e pineevent.Event) bool { return e.Kind == "request" && e.DestinationHost == host })
		return len(found) > 0
	})
	return found[len(found)-1]
}

// The first route answers host_unreachable after ~3 s while the next connects
// at once: the hedge serves after the delay and the loser is not recorded.
func TestHedgeServesFromTheNextRouteAfterTheDelay(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(100 * time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	slow := &slowTransport{delay: 2 * time.Second, err: socksReply(4)}
	fast := &slowTransport{}
	slowNode := pineNode("er_hedge_slow_"+id, slow)
	r := newTestRouter(slowNode, pineNode("er_hedge_fast_"+id, fast))
	host := "hedged-" + id + ".example"
	took, err := timedDial(t, r, host+":443")
	if err != nil || took > time.Second {
		t.Fatalf("hedged request took %s (err %v), want about the hedge delay", took, err)
	}
	if gap := time.Duration(fast.firstDial.Load() - slow.firstDial.Load()); gap < 50*time.Millisecond {
		t.Fatalf("the hedge started %s after the first route, want about the 100 ms delay", gap)
	}
	event := requestEvent(t, events, host)
	if event.Hedge != "won" || event.Outcome != "success" || event.Attempts != 2 || event.RouteID != "er_hedge_fast_"+id {
		t.Fatalf("request event %+v", event)
	}
	waitFor(t, "loser cancelled", func() bool { return slow.canceled.Load() == 1 })
	if slowNode.Marker().Count() != 0 {
		t.Fatal("a cancelled hedge loser was marked failed")
	}
	if pineroute.Skip(pineroute.WithAttempts(context.Background()), slowNode, "tcp", host+":443") {
		t.Fatal("a cancelled hedge loser left a refusal behind")
	}
	for _, e := range events.match(func(e pineevent.Event) bool { return e.Kind == "attempt" && e.DestinationHost == host }) {
		if e.RouteID == "er_hedge_slow_"+id {
			t.Fatalf("an attempt event was emitted for the cancelled loser: %+v", e)
		}
	}
}

func TestHedgeOffKeepsSequentialFailover(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(0)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	slow := &slowTransport{delay: 200 * time.Millisecond, err: socksReply(4)}
	fast := &slowTransport{}
	r := newTestRouter(pineNode("er_seq_slow_"+id, slow), pineNode("er_seq_fast_"+id, fast))
	host := "seq-" + id + ".example"
	took, err := timedDial(t, r, host+":443")
	if err != nil || took < 200*time.Millisecond {
		t.Fatalf("sequential failover took %s (err %v)", took, err)
	}
	if event := requestEvent(t, events, host); event.Hedge != "" || event.Outcome != "managed_failover_success" || event.Attempts != 2 {
		t.Fatalf("request event %+v", event)
	}
}

// The first route answers inside the race: it serves and the hedge is the
// loser. A connection the loser still produces is closed.
func TestHedgeLosesToAFirstRouteThatAnswers(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(100 * time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	first := &slowTransport{delay: 600 * time.Millisecond}
	hedge := &slowTransport{delay: 2 * time.Second, ignoreCancel: true}
	r := newTestRouter(pineNode("er_lost_a_"+id, first), pineNode("er_lost_b_"+id, hedge))
	host := "lost-" + id + ".example"
	if _, err := timedDial(t, r, host+":443"); err != nil {
		t.Fatal(err)
	}
	if event := requestEvent(t, events, host); event.Hedge != "lost" || event.Outcome != "success" || event.RouteID != "er_lost_a_"+id {
		t.Fatalf("request event %+v", event)
	}
	waitFor(t, "loser connection closed", func() bool { return hedge.closed.Load() >= 1 })
}

// A first route that fails during the race is recorded as usual; the hedge
// serves and the request is a failover.
func TestFirstRouteFailingDuringTheRaceIsRecorded(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(100 * time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	first := &slowTransport{delay: 500 * time.Millisecond, err: socksReply(5)}
	hedge := &slowTransport{delay: 1500 * time.Millisecond}
	firstNode := pineNode("er_race_a_"+id, first)
	r := newTestRouter(firstNode, pineNode("er_race_b_"+id, hedge))
	host := "race-" + id + ".example"
	if _, err := timedDial(t, r, host+":443"); err != nil {
		t.Fatal(err)
	}
	if event := requestEvent(t, events, host); event.Hedge != "won" || event.Outcome != "managed_failover_success" {
		t.Fatalf("request event %+v", event)
	}
	if !pineroute.Skip(pineroute.WithAttempts(context.Background()), firstNode, "tcp", host+":443") {
		t.Fatal("the first route's refusal during the race was not recorded")
	}
}

// Both racers fail; the request continues on the next route.
func TestBothRacersFailingContinuesSequentially(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(100 * time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	a := &slowTransport{delay: 600 * time.Millisecond, err: socksReply(4)}
	b := &slowTransport{delay: 600 * time.Millisecond, err: socksReply(4)}
	c := &slowTransport{}
	r := newTestRouter(pineNode("er_both_a_"+id, a), pineNode("er_both_b_"+id, b), pineNode("er_both_c_"+id, c))
	host := "both-" + id + ".example"
	if _, err := timedDial(t, r, host+":443"); err != nil {
		t.Fatal(err)
	}
	if event := requestEvent(t, events, host); event.Hedge != "failed" || event.Attempts != 3 || event.RouteID != "er_both_c_"+id {
		t.Fatalf("request event %+v", event)
	}
}

// A hedge stays in the first route's source list and never dials direct.
func TestHedgeStaysInTheFirstRoutesList(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(50 * time.Millisecond)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	slow := &slowTransport{delay: 400 * time.Millisecond}
	slowNode := pineNode("er_list_a_"+id, slow)
	slowNode.Options().Metadata = mdx.NewMetadata(map[string]any{"pine_route_id": "er_list_a_" + id, "pine_source_list_id": "epl_a"})
	otherList := pineNode("er_list_b_"+id, &slowTransport{})
	otherList.Options().Metadata = mdx.NewMetadata(map[string]any{"pine_route_id": "er_list_b_" + id, "pine_source_list_id": "epl_b"})
	direct := corechain.NewNode("direct-"+id, "",
		corechain.TransportNodeOption(&slowTransport{}),
		corechain.MetadataNodeOption(mdx.NewMetadata(map[string]any{"pine_route_kind": "direct", "backup": true})))
	for name, r := range map[string]*xchain.Router{
		"other list": newTestRouter(slowNode, otherList),
		"direct":     newTestRouter(slowNode, direct),
	} {
		host := "nohedge-" + name[:1] + "-" + id + ".example"
		if _, err := timedDial(t, r, host+":443"); err != nil {
			t.Fatal(err)
		}
		if event := requestEvent(t, events, host); event.Hedge != "" || event.Attempts != 1 || event.RouteID != "er_list_a_"+id {
			t.Fatalf("%s: request event %+v", name, event)
		}
	}
}

// Lost hedges on six distinct hosts eject the slow route; three do not.
func TestRouteLosingHedgesIsEjectedAsSlow(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(100 * time.Millisecond)()
	defer pineroute.SetTimingForTest(time.Minute/2, time.Second)()
	events := captureEvents(t)
	id := fmt.Sprint(time.Now().UnixNano())
	slow := &slowTransport{delay: 1500 * time.Millisecond, err: socksReply(4)}
	nodes := []*corechain.Node{pineNode("er_slowroute_"+id, slow)}
	for i := 0; i < 3; i++ {
		nodes = append(nodes, pineNode(fmt.Sprintf("er_slowok%d_%s", i, id), &slowTransport{}))
	}
	r := newTestRouter(nodes...)
	for i := 0; i < 6; i++ {
		if _, err := timedDial(t, r, fmt.Sprintf("slowhost-%d-%s.example:443", i, id)); err != nil {
			t.Fatal(err)
		}
		if ejected := events.routeEvents("route_ejected", "er_slowroute_"+id); len(ejected) != 0 && i < 5 {
			t.Fatalf("ejected after %d lost hedges: %+v", i+1, ejected)
		}
	}
	ejected := events.routeEvents("route_ejected", "er_slowroute_"+id)
	if len(ejected) != 1 || ejected[0].Reason != pineroute.ReasonSlow {
		t.Fatalf("ejections %+v, want one with reason slow", ejected)
	}
	before := slow.connects.Load()
	if _, err := timedDial(t, r, "after-"+id+".example:443"); err != nil {
		t.Fatal(err)
	}
	if slow.connects.Load() != before {
		t.Fatal("the slow route was still tried first after its ejection")
	}
}

// Many concurrent hedged requests: no races, no leaked connections.
func TestConcurrentHedgedRequests(t *testing.T) {
	defer pineroute.SetHedgeDelayForTest(5 * time.Millisecond)()
	id := fmt.Sprint(time.Now().UnixNano())
	a := &slowTransport{delay: 20 * time.Millisecond, ignoreCancel: true}
	b := &slowTransport{delay: 15 * time.Millisecond, ignoreCancel: true}
	r := newTestRouter(pineNode("er_conc_a_"+id, a), pineNode("er_conc_b_"+id, b))
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := timedDial(t, r, fmt.Sprintf("conc-%d-%s.example:443", i, id)); err != nil {
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d concurrent hedged requests failed", failures.Load())
	}
	// Every connection either served (closed by the caller) or lost and was
	// closed by the router, including losers that ignored cancellation.
	waitFor(t, "every dialed connection closed", func() bool {
		return a.closed.Load()+b.closed.Load() == a.dials.Load()+b.dials.Load()
	})
}
