package analysys

import (
	"io"
	"path/filepath"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

func (self RowsPrinter) Print(file string, line int, row Row) error {
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

func (self RowsPrinter) Sink() ScanSink {
	return ScanSink { self.Print, nil, false}
}

type RowsPrinter struct {
	w io.Writer
	ts Timestamp
	verify bool
	dry bool
}

func FolderDump(path string, conc int, w io.Writer, verify, dry bool) error {
	printer := RowsPrinter {w, Timestamp(0), verify, dry}
	return FolderScan(path, conc, printer.Sink())
}

func FolderScan(in string, conc int, sink ScanSink) error {
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
	return FilesScan(ins, conc, sink)
}

func FilesScan(files []string, conc int, sink ScanSink) error {
	cache, err := CacheLoad(files)
	if err != nil {
		return err
	}
	defer cache.Close()

	jobs, err := cache.All()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}

	queue := make(chan Range, conc)

	go func() {
		for _, job := range jobs {
			queue <-job
		}
	}()

	type LoadedBlock struct {
		File string
		Block Block
	}
	blocks := make(chan LoadedBlock, conc)

	errs := make(chan error, conc)

	for i := 0; i < conc; i++ {
		go func() {
			for job := range queue {
				block, err := job.Indexing.Load(job.Block)
				if err == nil {
					blocks <-LoadedBlock {job.File, block}
				} else {
					blocks <-LoadedBlock {}
				}
				errs <-err
			}
		}()
	}

	es := ""
	go func() {
		for _ = range jobs {
			eg := <-errs
			if eg != nil {
				es += eg.Error() + ";"
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(len(jobs))
	go func() {
		for block := range blocks {
			if sink.IsByBlock {
				sink.ByBlock(block.File, block.Block)
			} else {
				for i, row := range block.Block {
					err = sink.ByRow(block.File, i, row)
					if err != nil {
						break
					}
				}
			}
			wg.Done()
		}
	}()

	wg.Wait()

	if len(es) != 0 {
		if len(es) > 1024 {
			es = es[0: 1024] + "..."
		}
		return fmt.Errorf("partially failed: %s", es)
	}
	return err
}

type ScanSink struct {
	ByRow func(file string, line int, row Row) error
	ByBlock func(file string, block Block) error
	IsByBlock bool
}

func (self Cache) All() ([]Range, error) {
	return self.Find(TimestampNoBound, TimestampNoBound)
}

func (self Cache) Find(lower, upper TimestampBound) ([]Range, error) {
	ranges := make([]Range, 0)
	for _, file := range self.files {
		indexing, err := self.indexing(file)
		if err != nil {
			return nil, err
		}
		blocks := indexing.Interset(lower, upper)
		for i := range blocks {
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

func PartDump(path string, conc int, w io.Writer, verify, dry bool) error {
	if conc == 1 {
		return PartDumpSync(path, w, verify)
	}
	printer := RowsPrinter {w, Timestamp(0), verify, dry}
	return FilesScan([]string {path}, conc, printer.Sink())
}

func PartDumpSync(path string, w io.Writer, verify bool) error {
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
