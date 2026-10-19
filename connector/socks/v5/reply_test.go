package v5

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/go-gost/gosocks5"
)

func buildReply(rep uint8, host, port string) []byte {
	addr := gosocks5.Addr{}
	if err := addr.ParseFrom(net.JoinHostPort(host, port)); err != nil {
		panic(err)
	}
	var b [262]byte
	b[0], b[1], b[2] = gosocks5.Ver5, rep, 0
	n, _ := addr.Encode(b[3:])
	return b[:3+n]
}

func TestReadReplyPreservesCoalescedPayload(t *testing.T) {
	payload := []byte("server-first-data")
	c := newReplyConn(&streamConn{data: append(buildReply(gosocks5.Succeeded, "0.0.0.0", "0"), payload...)})

	rep, err := readReply(c)
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if rep.Rep != gosocks5.Succeeded {
		t.Fatalf("Rep = %d, want %d", rep.Rep, gosocks5.Succeeded)
	}

	rest, _ := io.ReadAll(c)
	if !bytes.Equal(rest, payload) {
		t.Fatalf("remaining = %q, want %q", rest, payload)
	}
}

// TestReadReplyCoalescedPayloadAgainstUpstream pins the defect this helper
// exists to avoid: gosocks5.ReadReply over-reads and drops the payload. If
// this fails, upstream gosocks5 was fixed and the local helper can go.
func TestReadReplyCoalescedPayloadAgainstUpstream(t *testing.T) {
	payload := []byte("server-first-data")
	stream := bytes.NewBuffer(append(buildReply(gosocks5.Succeeded, "0.0.0.0", "0"), payload...))

	if _, err := gosocks5.ReadReply(stream); err != nil {
		t.Fatalf("gosocks5.ReadReply: %v", err)
	}
	if rest, _ := io.ReadAll(stream); len(rest) != 0 {
		t.Fatalf("gosocks5.ReadReply preserved %q: upstream is fixed, remove the local helper", rest)
	}
}

func TestReadReplyAddressTypes(t *testing.T) {
	tests := []struct{ name, host, port string }{
		{"ipv4", "1.2.3.4", "8080"},
		{"ipv6", "2001:db8::1", "443"},
		{"domain", "proxy.example.com", "1080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep, err := readReply(bytes.NewReader(buildReply(gosocks5.Succeeded, tt.host, tt.port)))
			if err != nil {
				t.Fatalf("readReply: %v", err)
			}
			want := net.JoinHostPort(tt.host, tt.port)
			if ip := net.ParseIP(tt.host); ip != nil {
				want = net.JoinHostPort(ip.String(), tt.port)
			}
			if got := rep.Addr.String(); got != want {
				t.Fatalf("Addr = %q, want %q", got, want)
			}
		})
	}
}

func TestReadReplyFragmented(t *testing.T) {
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	whole := append(buildReply(gosocks5.Succeeded, "203.0.113.7", "3128"), payload...)

	for chunk := 1; chunk <= len(whole); chunk++ {
		c := newReplyConn(&streamConn{data: whole, chunk: chunk})
		rep, err := readReply(c)
		if err != nil {
			t.Fatalf("chunk %d: readReply: %v", chunk, err)
		}
		if rep.Addr.Host != "203.0.113.7" || rep.Addr.Port != 3128 {
			t.Fatalf("chunk %d: Addr = %v", chunk, rep.Addr)
		}
		if rest, _ := io.ReadAll(c); !bytes.Equal(rest, payload) {
			t.Fatalf("chunk %d: remaining = %v, want %v", chunk, rest, payload)
		}
	}
}

func TestReadReplyRejected(t *testing.T) {
	rep, err := readReply(bytes.NewReader(buildReply(gosocks5.NotAllowed, "0.0.0.0", "0")))
	if err != nil {
		t.Fatalf("readReply: %v", err)
	}
	if rep.Rep != gosocks5.NotAllowed {
		t.Fatalf("Rep = %d, want %d", rep.Rep, gosocks5.NotAllowed)
	}
}

func TestReadReplyShortAndBad(t *testing.T) {
	valid := buildReply(gosocks5.Succeeded, "127.0.0.1", "1")
	for n := 0; n < len(valid); n++ {
		if _, err := readReply(bytes.NewReader(valid[:n])); err == nil {
			t.Fatalf("len %d: expected error", n)
		}
	}

	badVer := append([]byte{}, valid...)
	badVer[0] = 4
	if _, err := readReply(bytes.NewReader(badVer)); err != gosocks5.ErrBadVersion {
		t.Fatalf("bad version: err = %v", err)
	}

	badType := append([]byte{}, valid...)
	badType[3] = 9
	if _, err := readReply(bytes.NewReader(badType)); err != gosocks5.ErrBadAddrType {
		t.Fatalf("bad type: err = %v", err)
	}
}

// streamConn is a net.Conn over a byte slice that returns at most chunk bytes
// per Read, like a TCP stream.
type streamConn struct {
	net.Conn
	data  []byte
	chunk int
}

func (c *streamConn) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := len(c.data)
	if c.chunk > 0 && n > c.chunk {
		n = c.chunk
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, c.data[:n])
	c.data = c.data[n:]
	return n, nil
}
