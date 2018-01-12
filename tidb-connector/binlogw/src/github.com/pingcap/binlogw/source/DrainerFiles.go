package source

import (
	"os"
)

func NewSourceFromPath(path string) (source Source, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if info.IsDir() {
		source, err = NewDrainerFiles(path)
	} else {
		source, err = NewDrainerFile(path, 1)
	}
	return
}

type DrainerFiles struct {
	path string
}

func NewDrainerFiles(path string) (*DrainerFiles, error) {
	return &DrainerFiles{path}, nil
}

func (self *DrainerFiles) Poll() (out chan BinlogEvents) {
	// TODO
	return nil
}

func (self *DrainerFiles) Close() error {
	return nil
}


