package source

import (
	"io"
	"os"
	"github.com/pingcap/tipb/go-binlog"
)

type DrainerFile struct {
	path string
	batchSize int
	file *os.File
}

func NewDrainerFile(path string, batchSize int) (self *DrainerFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	self = &DrainerFile{path, batchSize, file}
	return
}

func (self *DrainerFile) Poll() (out chan BinlogEvents) {
	out = make(chan BinlogEvents, 2)

	go func() {
		from := binlog.Pos{}
		decoder := NewDecoder(from, self.file)
		ent := &binlog.Entity{}
		bl := binlog.Binlog{}

		for (true) {
			ents := []binlog.Binlog{}
			index := 0
			var err error

			for ; index < self.batchSize; index++ {
				err = decoder.Decode(ent)
				if err != nil {
					break
				}
				bl.Unmarshal(ent.Payload)
				ents = append(ents, bl)
			}

			// TODO: if this file is writing, may cause a error, but is not a real error

			if (err == io.EOF) {
				out <- BinlogEvents{ents, nil}
				break
			} else if err != nil {
				out <- BinlogEvents{nil, err}
				break
			} else {
				out <- BinlogEvents{ents, nil}
			}
		}

		close(out)
	}()

	return out
}

func (self *DrainerFile) Close() error {
	return self.file.Close()
}


