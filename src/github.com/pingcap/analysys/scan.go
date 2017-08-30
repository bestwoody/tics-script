package analysys

import (
	"io"
	"path/filepath"
	"fmt"
	"os"
	"sort"
	"strings"
)

func FolderDump(path string, backlog int, w io.Writer, verify bool) error {
	ts := Timestamp(0)
	return FolderScan(path, backlog, func(file string, line int, row Row) error {
		if verify && row.Ts < ts {
			return fmt.Errorf("backward timestamp, file:%v line:%v %s", file, line, row.String())
		}
		ts = row.Ts
		_, err := w.Write([]byte(fmt.Sprintf("%s\n", row.String())))
		if err != nil {
			return err
		}
		return nil
	})
}

func FolderScan(in string, backlog int, fun func(file string, line int, row Row) error) error {
	ins := make([]string, 0)
	err := filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == in || strings.HasSuffix(path, IndexFileSuffix) {
			return nil
		}
		ins = append(ins, path)
		return nil
	})
	if err != nil {
		return err
	}

	sort.Strings(ins)

	cache, err := CacheLoad(ins)
	if err != nil {
		return err
	}
	defer cache.Close()

	ranges, err := cache.All()
	if err != nil {
		return err
	}

	// TODO: parallel, cache, pipeline

	for _, rg := range(ranges) {
		block, err := rg.Indexing.Load(rg.Block)
		if err != nil {
			return err
		}
		for i, row := range block {
			err = fun(rg.File, i, row)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (self Cache) All() ([]Range, error) {
	return self.Find(TimestampNoBound, TimestampNoBound)
}

func (self Cache) Find(lower, upper TimestampBound) ([]Range, error) {
	ranges := make([]Range, 0)
	for _, file := range(self.files) {
		indexing, err := self.indexing(file)
		if err != nil {
			return nil, err
		}
		blocks := indexing.Interset(lower, upper)
		for i := range(blocks) {
			ranges = append(ranges, Range {file, indexing, i})
		}
	}
	return ranges, nil
}

func (self Cache) indexing(path string) (*Indexing, error) {
	indexing, ok := self.indexings[path]
	if !ok {
		var err error
		indexing, err = IndexingLoad(path)
		if err != nil {
			return nil, err
		}
		self.indexings[path] = indexing
	}
	return indexing, nil
}

func (self Cache) Close() error {
	var err error
	for _, indexing := range self.indexings {
		e := indexing.Close()
		if e != nil {
			err = e
		}
	}
	return err
}

func CacheLoad(files []string) (Cache, error) {
	return Cache {files, make(map[string]*Indexing, 0)}, nil
}

// TODO: cache freq blocks
type Cache struct {
	files []string
	indexings map[string]*Indexing
}

type Range struct {
	File string
	Indexing *Indexing
	Block int
}

func PartDump(path string, w io.Writer, verify bool) error {
	indexing, err := IndexingLoad(path)
	if err != nil {
		return err
	}
	ts := Timestamp(0)
	for i := 0; i < indexing.Blocks(); i++ {
		block, err := indexing.Load(i)
		if err != nil {
			return err
		}
		for j, row := range block {
			if verify && row.Ts < ts {
				return fmt.Errorf("backward timestamp: block: %v row:%v %s", i, j, row.String())
			}
			_, err := w.Write([]byte(fmt.Sprintf("%s\n", row.String())))
			if err != nil {
				return err
			}
			ts = row.Ts
		}
	}
	return nil
}

func IndexDump(path string, w io.Writer) error {
	index, err := IndexLoad(path)
	for _, entry := range index {
		_, err := w.Write([]byte(fmt.Sprintf("%v %v\n", entry.Ts, entry.Offset)))
		if err != nil {
			return err
		}
	}
	return err
}

func IndexToBlockCheck(path string, w io.Writer) error {
	indexing, err := IndexingLoad(path)
	if err != nil {
		return err
	}
	for i := 0; i < indexing.Blocks(); i++ {
		_, err := indexing.Load(i)
		if err != nil {
			return err
		}
	}
	return nil
}
