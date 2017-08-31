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

func FolderDump(path string, conc int, pred Predicate, w io.Writer, verify, dry bool) error {
	printer := RowsPrinter {w, Timestamp(0), verify, dry}
	return FolderScan(path, conc, pred, printer.Sink())
}

func FolderScan(in string, conc int, pred Predicate, sink ScanSink) error {
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
	return FilesScan(ins, conc, pred, sink)
}

func FilesScan(files []string, conc int, pred Predicate, sink ScanSink) error {
	cache, err := CacheLoad(files)
	if err != nil {
		return err
	}
	defer cache.Close()

	jobs, err := cache.Find(pred.Lower, pred.Upper)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}

	queue := make(chan UnloadBlock, conc)

	go func() {
		for _, job := range jobs {
			queue <-job
		}
	}()

	blocks := make(chan LoadedBlock, conc * 2)
	errs := make(chan error, conc)

	for i := 0; i < conc; i++ {
		go func() {
			for job := range queue {
				block, err := job.Indexing.Load(job.Order)
				if err == nil {
					blocks <-LoadedBlock {job.File, job.Order, block}
				} else {
					blocks <-LoadedBlock {}
				}
				errs <-err
			}
		}()
	}

	var es string
	go func() {
		for _ = range jobs {
			eg := <-errs
			if eg != nil {
				if len(es) > 1024 {
					es += "..."
				} else {
					es += eg.Error() + ";"
				}
			}
		}
	}()

	reordereds := make(chan LoadedBlock, conc)
	reorderer := NewBlockReorderer(jobs)
	go func() {
		for block := range blocks {
			reorderer.Push(block)
			reordereds <-reorderer.Pop()
			//for reorderer.Ready() {
			//	reordereds <-reorderer.Pop()
			//}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(len(jobs))
	go func() {
		for block := range reordereds {
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
		return fmt.Errorf("partially failed: %s", es)
	}
	return nil
}

func (self *BlockReorderer) Push(block LoadedBlock) {
	self.stage = append(self.stage, block)
}

func (self *BlockReorderer) Ready() bool {
	return len(self.stage) > 0
}

func (self *BlockReorderer) Pop() LoadedBlock {
	return self.stage[0]
}

func NewBlockReorderer(origin []UnloadBlock) *BlockReorderer {
	return &BlockReorderer {origin, make([]LoadedBlock, 0)}
}

// TODO: use priority queue
type BlockReorderer struct {
	origin []UnloadBlock
	stage []LoadedBlock
}

type LoadedBlock struct {
	File string
	Order int
	Block Block
}

type ScanSink struct {
	ByRow func(file string, line int, row Row) error
	ByBlock func(file string, block Block) error
	IsByBlock bool
}

func (self Cache) All() ([]UnloadBlock, error) {
	return self.Find(TimestampNoBound, TimestampNoBound)
}

func (self Cache) Find(lower, upper TimestampBound) ([]UnloadBlock, error) {
	ranges := make([]UnloadBlock, 0)
	for _, file := range self.files {
		indexing, err := self.indexing(file)
		if err != nil {
			return nil, err
		}
		blocks := indexing.Interset(lower, upper)
		for i := range blocks {
			ranges = append(ranges, UnloadBlock {file, i, indexing})
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

type UnloadBlock struct {
	File string
	Order int
	Indexing *Indexing
}

func PartDump(path string, conc int, pred Predicate, w io.Writer, verify, dry bool) error {
	if conc == 1 {
		return PartDumpSync(path, w, verify)
	}
	printer := RowsPrinter {w, Timestamp(0), verify, dry}
	return FilesScan([]string {path}, conc, pred, printer.Sink())
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

type Predicate struct {
	Lower TimestampBound
	Upper TimestampBound
}
