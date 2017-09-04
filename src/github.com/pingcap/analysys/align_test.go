package analysys

import (
	"testing"
	"unsafe"
)

func TestRow(t *testing.T) {
	var row Row
	var info RowInfo
	if int(unsafe.Sizeof(info)) != int(row.PersistSize()) {
		t.Fatal("row mem not aligned: %d != %d", int(unsafe.Sizeof(info)), int(row.PersistSize()))
	}
}

func TestPageEventRing(t *testing.T) {
	var ring PageEventRing
	if int(unsafe.Sizeof(ring)) != 4 {
		t.Fatal("ring mem not aligned: %d != %d", int(unsafe.Sizeof(ring)), 4)
	}
}

func TestPageUser(t *testing.T) {
	var user PageUser
	if int(unsafe.Sizeof(user)) != 8 {
		t.Fatal("user mem not aligned: %d != %d", int(unsafe.Sizeof(user)), 8)
	}
}
