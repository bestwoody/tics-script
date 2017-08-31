package analysys

import (
	"encoding/binary"
	"errors"
	"io"
	"fmt"
)

func (self *RowPrinter) Print(file string, line int, row Row) error {
	if self.verify && row.Ts < self.ts {
		return fmt.Errorf("backward timestamp, file:%v line:%v %s", file, line, row.String())
	}
	self.ts = row.Ts
	if !self.dry {
		_, err := self.w.Write([]byte(fmt.Sprintf("%s\n", row.String())))
		if err != nil {
			return err
		}
	}
	return nil
}

func (self RowPrinter) Sink() ScanSink {
	return ScanSink {self.Print, nil, false}
}

type RowPrinter struct {
	w io.Writer
	ts Timestamp
	verify bool
	dry bool
}

func (self *Row) Write(w io.Writer) (uint32, error) {
	written := uint32(0)
	err := binary.Write(w, binary.LittleEndian, self.Ts)
	if err != nil {
		return written, err
	}
	written += TimestampLen

	err = binary.Write(w, binary.LittleEndian, self.Id)
	if err != nil {
		return written, err
	}
	written += 4

	err = binary.Write(w, binary.LittleEndian, self.Event)
	if err != nil {
		return written, err
	}
	written += 2

	err = binary.Write(w, binary.LittleEndian, uint16(len(self.Props)))
	if err != nil {
		return written, err
	}
	written += 2

	_, err = w.Write(self.Props)
	if err != nil {
		return written, errors.New("writing event props: " + err.Error())
	}
	written += uint32(len(self.Props))

	if written != self.PersistSize() {
		panic("Row.Write: wrong written size")
	}
	return written, nil
}

func (self *Row) PersistSize() uint32 {
	return uint32(TimestampLen + 4 + 2 + 2 + len(self.Props))
}

func RowLoad(r io.Reader, row *Row) error {
	err := binary.Read(r, binary.LittleEndian, &row.Ts)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.LittleEndian, &row.Id)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.LittleEndian, &row.Event)
	if err != nil {
		return err
	}
	cbp := uint16(0)
	err = binary.Read(r, binary.LittleEndian, &cbp)
	if err != nil {
		return err
	}
	row.Props = make([]byte, cbp)
	_, err = io.ReadFull(r, row.Props)
	if err != nil {
		err = fmt.Errorf("reading event props, size:%v %s", cbp, err.Error())
	}
	return err
}

func (self *Row) String() string {
	return fmt.Sprintf("%v %v %v %s", self.Ts, self.Id, self.Event, string(self.Props))
}

type Row struct {
	Ts Timestamp
	Id uint32
	Event uint16
	Props []byte
}
