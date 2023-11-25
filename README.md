# `wal`
[![GoDoc](https://godoc.org/github.com/nnsgmsone/wal?status.svg)](https://godoc.org/github.com/nnsgmsone/wal)

A fast write ahead log for Go, all writes wait for sync and all functions are thread safe.

## Getting Started

```go
// open a write ahead log file
w, err := Open("testlog")

// write some records
pos, err = w.Write("first record")
pos, err = w.Write("second record")
pos, err = w.Write("third record")

// read all records from head
r, err = w.NewReader(0)
for {
    pos, data, err = r.Next()
    if pos == -1{
        break
    }
}

// truncate the log from position before 100
err = w.TruncateBefore(100)
// truncate the log from position after 200
err = w.TruncateAfter(200)

// backup
name, err = w.Backup()

// recover the write ahead log from the backup file
err = w.RecoverFromBackup(name)

// close the log
w.Close()
```

## License


`wal` source code is available under the MIT [License](/LICENSE).
