package pineevent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	socketEnvironment = "PINE_GOST_EVENT_SOCKET"
	queueSize         = 1024
	writeTimeout      = 10 * time.Millisecond
)

// Event is the private, versioned data-plane event sent from GOST to Pine's
// session coordinator. It deliberately carries opaque route identifiers and
// never carries proxy addresses or credentials.
type Event struct {
	Version               int    `json:"v"`
	Kind                  string `json:"kind"`
	ObservedAtUnixMS      int64  `json:"observed_at_unix_ms"`
	ConnectionID          string `json:"connection_id,omitempty"`
	Network               string `json:"network,omitempty"`
	DestinationHost       string `json:"destination_host,omitempty"`
	DestinationPort       int    `json:"destination_port,omitempty"`
	RouteID               string `json:"route_id,omitempty"`
	SourceListID          string `json:"source_list_id,omitempty"`
	Tier                  string `json:"tier,omitempty"`
	RouteKind             string `json:"route_kind,omitempty"`
	Attempt               int    `json:"attempt,omitempty"`
	Attempts              int    `json:"attempts,omitempty"`
	Result                string `json:"result,omitempty"`
	Outcome               string `json:"outcome,omitempty"`
	ErrorClass            string `json:"error_class,omitempty"`
	SOCKS5Reply           string `json:"socks5_reply,omitempty"`
	DurationMS            int64  `json:"duration_ms,omitempty"`
	FirstDownstreamByteMS int64  `json:"first_downstream_byte_ms,omitempty"`
	BytesUp               int64  `json:"bytes_up,omitempty"`
	BytesDown             int64  `json:"bytes_down,omitempty"`
	UploadActiveMS        int64  `json:"upload_active_ms,omitempty"`
	DownloadActiveMS      int64  `json:"download_active_ms,omitempty"`
	DroppedEvents         uint64 `json:"dropped_events,omitempty"`
}

type Route struct {
	RouteID      string
	SourceListID string
	Tier         string
	Kind         string
}

type dialStateKey struct{}

type DialState struct {
	mu      sync.RWMutex
	route   Route
	outcome string
}

func ContextWithDialState(ctx context.Context) (context.Context, *DialState) {
	state := &DialState{}
	return context.WithValue(ctx, dialStateKey{}, state), state
}

func SetSelectedRoute(ctx context.Context, route Route, outcome string) {
	state, _ := ctx.Value(dialStateKey{}).(*DialState)
	if state == nil {
		return
	}
	state.mu.Lock()
	state.route = route
	state.outcome = outcome
	state.mu.Unlock()
}

func (s *DialState) SelectedRoute() (Route, string) {
	if s == nil {
		return Route{}, ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.route, s.outcome
}

type socks5ReplyError interface {
	SOCKS5ReplyCode() uint8
}

func ErrorDetails(err error) (class, reply string) {
	if err == nil {
		return "", ""
	}

	var socksErr socks5ReplyError
	if errors.As(err, &socksErr) {
		return "socks5_reply", socks5ReplyName(socksErr.SOCKS5ReplyCode())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", ""
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout", ""
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns", ""
	}
	return "connect_error", ""
}

func Destination(address string) (host string, port int) {
	host = address
	h, p, err := net.SplitHostPort(address)
	if err != nil {
		return host, 0
	}
	host = h
	port, _ = strconv.Atoi(p)
	return host, port
}

func socks5ReplyName(code uint8) string {
	switch code {
	case 1:
		return "general_failure"
	case 2:
		return "not_allowed"
	case 3:
		return "network_unreachable"
	case 4:
		return "host_unreachable"
	case 5:
		return "connection_refused"
	case 6:
		return "ttl_expired"
	case 7:
		return "command_unsupported"
	case 8:
		return "address_unsupported"
	default:
		return "unknown"
	}
}

type emitter struct {
	path    string
	queue   chan Event
	dropped atomic.Uint64
}

var defaultEmitter = newEmitter(os.Getenv(socketEnvironment))

func Emit(event Event) {
	defaultEmitter.emit(event)
}

func newEmitter(path string) *emitter {
	e := &emitter{path: path}
	if path == "" {
		return e
	}
	e.queue = make(chan Event, queueSize)
	go e.run()
	return e
}

func (e *emitter) emit(event Event) {
	if e == nil || e.queue == nil {
		return
	}
	if event.Version == 0 {
		event.Version = 1
	}
	if event.ObservedAtUnixMS == 0 {
		event.ObservedAtUnixMS = time.Now().UnixMilli()
	}
	select {
	case e.queue <- event:
	default:
		// Quality telemetry is best-effort and must never backpressure traffic.
		e.dropped.Add(1)
	}
}

func (e *emitter) run() {
	var connection *net.UnixConn
	for event := range e.queue {
		dropped := e.dropped.Swap(0)
		event.DroppedEvents = dropped
		payload, err := json.Marshal(event)
		if err != nil {
			e.dropped.Add(dropped + 1)
			continue
		}
		if connection == nil {
			connection, _ = net.DialUnix("unixgram", nil, &net.UnixAddr{Name: e.path, Net: "unixgram"})
			if connection == nil {
				e.dropped.Add(dropped + 1)
				continue
			}
		}
		_ = connection.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := connection.Write(payload); err != nil {
			e.dropped.Add(dropped + 1)
			_ = connection.Close()
			connection = nil
		}
	}
}
