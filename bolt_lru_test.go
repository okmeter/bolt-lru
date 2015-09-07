package boltlru

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestLRU(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	l, err := New(128, path.Join(tmpDir, "db"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		kv := fmt.Sprintf("%d", i)
		err := l.Add(kv, []byte(kv))
		if err != nil {
			t.Fatalf("failed to add value: %s", err)
		}
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	for i := 0; i < 128; i++ {
		k := fmt.Sprintf("%d", i)
		_, err := l.Get(k)
		if err == nil {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		k := fmt.Sprintf("%d", i)
		v, err := l.Get(k)
		if err != nil {
			t.Fatalf("should not be evicted")
		}
		if k != string(v) {
			t.Fatalf("invalid value")
		}
	}
	l.db.Close()

	l, err = New(128, path.Join(tmpDir, "db"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for i := 128; i < 256; i++ {
		k := fmt.Sprintf("%d", i)
		v, err := l.Get(k)
		if err != nil {
			t.Fatalf("should not be evicted")
		}
		if k != string(v) {
			t.Fatalf("invalid value")
		}
	}

}
