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
