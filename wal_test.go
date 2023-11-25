package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testRecordNum = 10
)

type testRecord struct {
	pos  int64
	data []byte
}

func TestWal(t *testing.T) {
	testRecords := make([]testRecord, testRecordNum)
	for i := 0; i < testRecordNum; i++ {
		testRecords[i].data = []byte(fmt.Sprintf("record%v", i))
	}
	w, err := Open("test.log", 0664)
	require.NoError(t, err)
	for i := range testRecords {
		pos, err := w.Write(testRecords[i].data)
		require.NoError(t, err)
		testRecords[i].pos = pos
	}
	r, err := w.NewReader(0)
	require.NoError(t, err)
	for i := 0; ; i++ {
		pos, data, err := r.Next()
		require.NoError(t, err)
		if pos == -1 {
			break
		}
		require.Equal(t, testRecords[i].pos, pos)
		require.Equal(t, testRecords[i].data, data)
	}
	r.Close()
	err = w.TruncateAfter(testRecords[8].pos)
	require.NoError(t, err)
	r, err = w.NewReader(0)
	require.NoError(t, err)
	i := 0
	for ; ; i++ {
		pos, data, err := r.Next()
		require.NoError(t, err)
		if pos == -1 {
			break
		}
		require.Equal(t, testRecords[i].pos, pos)
		require.Equal(t, testRecords[i].data, data)
	}
	r.Close()
	require.Equal(t, 8, i)
	err = w.TruncateBefore(testRecords[3].pos)
	require.NoError(t, err)
	r, err = w.NewReader(0)
	require.NoError(t, err)
	i = 3
	for ; ; i++ {
		pos, data, err := r.Next()
		require.NoError(t, err)
		if pos == -1 {
			break
		}
		require.Equal(t, testRecords[i].data, data)
	}
	r.Close()
	require.Equal(t, 8, i)
	err = w.Close()
	require.NoError(t, err)
	// test recovery
	fp, err := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE, 0664)
	require.NoError(t, err)
	err = fp.Truncate(100)
	require.NoError(t, err)
	w, err = Open("test.log", 0664)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	os.Remove("test.log")
}

func TestBackup(t *testing.T) {
	testRecords := make([]testRecord, testRecordNum)
	for i := 0; i < testRecordNum; i++ {
		testRecords[i].data = []byte(fmt.Sprintf("record%v", i))
	}
	w, err := Open("test.log", 0664)
	require.NoError(t, err)
	for i := range testRecords {
		pos, err := w.Write(testRecords[i].data)
		require.NoError(t, err)
		testRecords[i].pos = pos
	}
	r, err := w.NewReader(0)
	require.NoError(t, err)
	for i := 0; ; i++ {
		pos, data, err := r.Next()
		require.NoError(t, err)
		if pos == -1 {
			break
		}
		require.Equal(t, testRecords[i].pos, pos)
		require.Equal(t, testRecords[i].data, data)
	}
	r.Close()
	name, err := w.Backup()
	require.NoError(t, err)
	require.NoError(t, err)
	err = w.RecoverFromBackup(name)
	require.NoError(t, err)
	r, err = w.NewReader(0)
	require.NoError(t, err)
	for i := 0; ; i++ {
		pos, data, err := r.Next()
		require.NoError(t, err)
		if pos == -1 {
			break
		}
		require.Equal(t, testRecords[i].pos, pos)
		require.Equal(t, testRecords[i].data, data)
	}
	r.Close()
	err = w.Close()
	require.NoError(t, err)
	os.Remove("test.log")
}

func BenchmarkWrite(b *testing.B) {
	w, err := Open("test.log", 0664)
	require.NoError(b, err)
	for i := 0; i < b.N; i++ {
		_, err := w.Write([]byte("test"))
		require.NoError(b, err)
	}
	err = w.Close()
	require.NoError(b, err)
	os.Remove("test.log")
}
