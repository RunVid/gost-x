package v5

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type partialWriteConn struct{ net.Conn }

func (partialWriteConn) Write([]byte) (int, error) { return 3, io.ErrShortWrite }

func TestUsageCountsActualPartialWrite(t *testing.T) {
	var usage atomic.Int64
	c := &countingConn{Conn: partialWriteConn{}, startedAt: time.Now(), usage: &usage}
	n, err := c.Write(make([]byte, 50))
	if n != 3 || err != io.ErrShortWrite || usage.Load() != 3 || c.BytesWritten() != 3 {
		t.Fatalf("write=%d/%v usage=%d local=%d", n, err, usage.Load(), c.BytesWritten())
	}
}

type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkRelayWriteCounter(b *testing.B) {
	for _, metered := range []bool{false, true} {
		name := "existing"
		if metered {
			name = "metered"
		}
		b.Run(name, func(b *testing.B) {
			var usage atomic.Int64
			c := &countingConn{Conn: discardConn{}, startedAt: time.Now()}
			if metered {
				c.usage = &usage
			}
			data := make([]byte, 32<<10)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = c.Write(data)
			}
		})
	}
}
