package v5

import (
	"bufio"
	"io"
	"net"

	"github.com/go-gost/gosocks5"
)

// replyConn buffers the connection for reply parsing so bytes read past a
// SOCKS5 reply stay readable by the relay. The buffer is filled with
// reply-sized reads, so packet-backed connections such as dialer/ftcp, whose
// Read maps to one PacketConn.ReadFrom, still yield a whole reply per call.
type replyConn struct {
	net.Conn
	r *bufio.Reader
}

func newReplyConn(c net.Conn) *replyConn {
	return &replyConn{Conn: c, r: bufio.NewReaderSize(c, 262)}
}

func (c *replyConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

// readReply reads exactly one SOCKS5 reply, leaving any following bytes
// buffered on r.
func readReply(r io.Reader) (*gosocks5.Reply, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if hdr[0] != gosocks5.Ver5 {
		return nil, gosocks5.ErrBadVersion
	}

	b := []byte{hdr[3]}
	var n int
	switch hdr[3] {
	case gosocks5.AddrIPv4:
		n = 6
	case gosocks5.AddrIPv6:
		n = 18
	case gosocks5.AddrDomain:
		var l [1]byte
		if _, err := io.ReadFull(r, l[:]); err != nil {
			return nil, err
		}
		b = append(b, l[0])
		n = int(l[0]) + 2
	default:
		return nil, gosocks5.ErrBadAddrType
	}

	b = append(b, make([]byte, n)...)
	if _, err := io.ReadFull(r, b[len(b)-n:]); err != nil {
		return nil, err
	}

	addr := new(gosocks5.Addr)
	if err := addr.Decode(b); err != nil {
		return nil, err
	}

	return &gosocks5.Reply{Rep: hdr[1], Addr: addr}, nil
}
