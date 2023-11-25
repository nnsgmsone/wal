package wal

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"unsafe"

	"github.com/google/uuid"
)

// Open open a new write ahead log
func Open(name string, filePerm os.FileMode) (*Wal, error) {
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	w := &Wal{
		fp:   fp,
		name: name,
		ch:   make(chan *request, SyncConcurrency),
	}
	size, err := w.fileSize()
	if err != nil {
		fp.Close()
		return nil, err
	}
	w.pos.Store(size)
	// recover from the last sync record
	if err := w.recovery(); err != nil {
		return nil, err
	}
	go w.syncLoop()
	return w, nil
}

// Close close the write ahead log
func (w *Wal) Close() error {
	w.Lock()
	defer w.Unlock()
	return w.fp.Close()
}

// NewReader returns a new reader for the write ahead log,
// the created reader traverses the current point snapshot
// to read all the records in the current write ahead log
func (w *Wal) NewReader(pos int64) (*Reader, error) {
	w.RLock()
	return &Reader{w: w, pos: pos, size: w.pos.Load()}, nil
}

// TruncateBefore truncates the front of the write ahead log by
// removing all records that are before the provided pos
// this operation is mutually exclusive with all read and write operations
func (w *Wal) TruncateBefore(pos int64) error {
	var err error

	w.Lock()
	defer w.Unlock()
	if err := w.backup("back", 0); err != nil {
		return err
	}
	defer w.removeBackup("back")
	if err := w.backup("temp", pos); err != nil {
		return err
	}
	if err := os.Remove(w.name); err != nil {
		return err
	}
	w.fp.Close()
	if err := os.Rename(w.name+".temp", w.name); err != nil {
		w.removeBackup("temp")
		if err := w.recoverFromBackup("back"); err != nil {
			panic(err)
		}
		return err
	}
	w.fp, err = os.OpenFile(w.name, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return err
	}
	size, err := w.fileSize()
	if err != nil {
		return err
	}
	w.pos.Store(size)
	return nil
}

// TruncateAfter truncates the back of the write ahead log by
// removing all records after the provided pos
// this operation is mutually exclusive with all read and write operations
func (w *Wal) TruncateAfter(pos int64) error {
	w.Lock()
	defer w.Unlock()
	if err := w.backup("back", 0); err != nil {
		return err
	}
	defer w.removeBackup("back")
	if err := w.fp.Truncate(pos); err != nil {
		return err
	}
	w.pos.Store(pos)
	if err := w.writeSyncRecord(); err != nil {
		if err := w.recoverFromBackup("back"); err != nil {
			panic(err)
		}
		return err
	}
	return nil
}

// write a record to the write ahead log,
// return the position of the record
func (w *Wal) Write(p []byte) (int64, error) {
	if len(p) == 0 {
		return -1, nil
	}
	if len(p) > MaxRecordSize {
		return -1, fmt.Errorf("Illegal record size: max size is %v", MaxRecordSize)
	}
	req := reqPool.Get().(*request)
	defer reqPool.Put(req)
	req.err = nil
	req.data = p
	req.wg.Add(1)
	w.RLock()
	defer w.RUnlock()
	w.ch <- req
	req.wg.Wait()
	return req.pos, req.err
}

// Backup backup the write ahead log, return the backup file name
// this operation is mutually exclusive with all read and write operations
func (w *Wal) Backup() (string, error) {
	w.Lock()
	defer w.Unlock()
	id := uuid.New()
	name := hex.EncodeToString(id[:])
	if err := w.backup(name, 0); err != nil {
		return "", err
	}
	return name, nil
}

// RecoverFromBackup recover the write ahead log from the backup
// this operation is mutually exclusive with all read and write operations
func (w *Wal) RecoverFromBackup(name string) error {
	if err := os.Remove(w.name); err != nil {
		return err
	}
	return w.recoverFromBackup(name)
}

// Close close the reader
func (r *Reader) Close() {
	r.w.RUnlock()
}

// Next read a record from the write ahead log,
// return a byte slice contains the entry and
// the position for the write ahead log, -1 means the last record
func (r *Reader) Next() (int64, []byte, error) {
	for {
		if r.pos == r.size {
			return -1, nil, nil
		}
		if err := r.readRecord(); err != nil {
			return -1, nil, err
		}
		if len(r.data) == 0 {
			continue
		}
		return r.pos - int64(len(r.data)+RecordHeaderSize), r.data, nil
	}
}

func (w *Wal) syncLoop() {
	reqs := make([]*request, 0, SyncConcurrency)
	for {
		select {
		case req := <-w.ch:
			reqs = append(reqs, req)
			if len(w.ch) > 0 {
				break
			}
			for _, req := range reqs {
				req.pos, req.err = w.writeRecord(req.data)
			}
			err := w.writeSyncRecord()
			for _, req := range reqs {
				if req.err != nil {
					req.err = err
				}
				req.wg.Done()
			}
			reqs = reqs[:0]
		}
	}
}

func (w *Wal) recovery() error {
	var pos int64

	r, err := w.NewReader(0)
	if err != nil {
		return nil
	}
	defer r.Close()
	if r.size == 0 {
		return nil
	}
	for {
		if r.size < r.pos+RecordHeaderSize { // incompleted data
			break
		}
		if err := r.readRecord(); err != nil {
			break
		}
		if r.h.size == 0 { // sync record
			pos = r.pos
		}
	}
	w.pos.Store(pos)
	return w.fp.Truncate(pos)
}

func (w *Wal) backup(suffix string, pos int64) error {
	size, err := w.fileSize()
	if err != nil {
		return err
	}
	name := w.name + "." + suffix
	src, err := os.OpenFile(w.name, os.O_RDWR, 0664)
	if err != nil {
		return err
	}
	defer src.Close()
	if off, err := src.Seek(pos, 0); err != nil {
		return err
	} else if off != pos {
		return errors.New("Backup failed")
	}
	dst, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer dst.Close()
	if n, err := io.Copy(dst, src); err != nil {
		return err
	} else if n != (size - pos) {
		return errors.New("Backup failed")
	}
	return nil
}

func (w *Wal) removeBackup(suffix string) error {
	return os.Remove(w.name + "." + suffix)
}

func (w *Wal) recoverFromBackup(suffix string) error {
	w.fp.Close()
	if err := os.Rename(w.name+"."+suffix, w.name); err != nil {
		return err
	}
	fp, err := os.OpenFile(w.name, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return err
	}
	w.fp = fp
	size, err := w.fileSize()
	if err != nil {
		return err
	}
	w.pos.Store(size)
	return nil
}

func (w *Wal) fileSize() (int64, error) {
	fstat, err := w.fp.Stat()
	if err != nil {
		return -1, err
	}
	return fstat.Size(), nil
}

func (w *Wal) writeRecord(p []byte) (int64, error) {
	var h recordHeader

	n := len(p)
	pos := w.pos.Load()
	h.size = uint32(n)
	h.sum = crc32.ChecksumIEEE(p)

	if n, err := w.fp.WriteAt(encode(&h), pos); err != nil {
		return -1, err
	} else if n != RecordHeaderSize {
		return -1, errors.New("Fail to write record")
	}
	if n, err := w.fp.WriteAt(p, pos+RecordHeaderSize); err != nil {
		return -1, err
	} else if n != len(p) {
		return -1, errors.New("Fail to write record")
	}
	w.pos.Add(int64(n + RecordHeaderSize))
	return pos, nil
}

func (w *Wal) writeSyncRecord() error {
	var h recordHeader

	if n, err := w.fp.WriteAt(encode(&h), w.pos.Load()); err != nil {
		return err
	} else if n != RecordHeaderSize {
		return errors.New("Fail to write sync record")
	}
	w.pos.Add(RecordHeaderSize)
	return w.fp.Sync()
}

func encode[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

func decode[T any](v []byte) T {
	return *(*T)(unsafe.Pointer(&v[0]))
}

func (r *Reader) readRecord() error {
	if err := r.readRecordHeader(); err != nil {
		return err
	}
	if cap(r.data) < int(r.h.size) {
		r.data = make([]byte, r.h.size)
	}
	r.data = r.data[:r.h.size]
	if n, err := r.w.fp.ReadAt(r.data, r.pos); err != nil {
		return err
	} else if n != len(r.data) {
		return errors.New("Fail to read record")
	}
	if crc32.ChecksumIEEE(r.data) != r.h.sum {
		return errors.New("Fail to read record: checksum is wrong, data is broken")
	}
	r.pos += int64(r.h.size)
	return nil
}

func (r *Reader) readRecordHeader() error {
	if n, err := r.w.fp.ReadAt(encode(&r.h), r.pos); err != nil {
		return err
	} else if n != RecordHeaderSize {
		return errors.New("Fail to read record")
	}
	r.pos += RecordHeaderSize
	return nil
}
