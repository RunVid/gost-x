// Package pineusage counts forwarded managed-proxy traffic for the private admin API.
// Process totals survive configuration reload; they are not reset when read.
package pineusage

import (
	"github.com/google/uuid"
	"sync/atomic"
)

var processID = uuid.NewString()
var Upload, Download atomic.Int64

type Snapshot struct {
	ProcessID     string `json:"process_id"`
	UploadBytes   int64  `json:"upload_bytes"`
	DownloadBytes int64  `json:"download_bytes"`
}

func Read() Snapshot {
	return Snapshot{ProcessID: processID, UploadBytes: Upload.Load(), DownloadBytes: Download.Load()}
}
