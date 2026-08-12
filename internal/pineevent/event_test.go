package pineevent

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testSOCKS5ReplyError uint8

func (e testSOCKS5ReplyError) Error() string          { return "rejected" }
func (e testSOCKS5ReplyError) SOCKS5ReplyCode() uint8 { return uint8(e) }

func TestErrorDetails(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantClass string
		wantReply string
	}{
		{name: "SOCKS host unreachable", err: testSOCKS5ReplyError(4), wantClass: "socks5_reply", wantReply: "host_unreachable"},
		{name: "unknown SOCKS reply", err: testSOCKS5ReplyError(9), wantClass: "socks5_reply", wantReply: "unknown"},
		{name: "deadline", err: context.DeadlineExceeded, wantClass: "timeout"},
		{name: "DNS", err: &net.DNSError{Err: "no such host", Name: "example.invalid"}, wantClass: "dns"},
		{name: "generic", err: errors.New("failed"), wantClass: "connect_error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotClass, gotReply := ErrorDetails(test.err)
			if gotClass != test.wantClass || gotReply != test.wantReply {
				t.Fatalf("ErrorDetails() = %q, %q; want %q, %q", gotClass, gotReply, test.wantClass, test.wantReply)
			}
		})
	}
}

func TestDestination(t *testing.T) {
	host, port := Destination("example.com:443")
	if host != "example.com" || port != 443 {
		t.Fatalf("Destination() = %q, %d", host, port)
	}
}

func TestEmitterWritesVersionedDatagram(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "pine-gost-event-test.sock")
	_ = os.Remove(socketPath)
	t.Cleanup(func() { _ = os.Remove(socketPath) })
	listener, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socketPath, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	emitter := newEmitter(socketPath)
	emitter.emit(Event{Kind: "attempt", DestinationHost: "example.com"})

	if err := listener.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 2048)
	n, _, err := listener.ReadFromUnix(payload)
	if err != nil {
		t.Fatal(err)
	}
	got := string(payload[:n])
	if got == "" || got[0] != '{' {
		t.Fatalf("event payload = %q", got)
	}
	if want := `"v":1`; !contains(got, want) {
		t.Fatalf("event payload %q does not contain %q", got, want)
	}
}

func contains(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}
