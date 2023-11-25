package wal

import (
	"os"
	"sync"
	"sync/atomic"
)

const (
	// MaxRecordSize is the maximum size of a record
	MaxRecordSize = 1<<32 - 1
	// SyncConcurrency is the number of concurrent syncs
	SyncConcurrency = 1 << 10
	// RecordHeaderSize is the size of the record header
	RecordHeaderSize = 8
)

// Reader represents a reader for the write ahead log,
// Reader is thread-unsafe
type Reader struct {
	w *Wal
	// read position for the next record
	pos int64
	// size of the write ahead log
	size int64
	// data is the buffer for read write ahead log
	data []byte
	// recordHeader is the header of a record
	h recordHeader
}

// WAL represents a write ahead log that provides durability
// and fault-tolerance for incoming writes
type Wal struct {
	sync.RWMutex
	name string
	fp   *os.File
	// byte position where the record is writen
	pos atomic.Int64
	ch  chan *request
}

type request struct {
	// record position
	pos  int64
	err  error
	data []byte
	wg   sync.WaitGroup
}

// recordHeader is the header of a record
type recordHeader struct {
	sum  uint32 // checksum of record
	size uint32 // size of record
}

// any is used to avoid allocation
var reqPool = sync.Pool{
	New: func() any {
		return new(request)
	},
}
