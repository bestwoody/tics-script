package analysys

import (
	"io"
	"path/filepath"
	"fmt"
	"container/heap"
	"os"
	"sort"
	"strings"
	"sync"
)

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

	// Wait for all jobs are done
	var wg sync.WaitGroup

	// Jobs to do
	queue := make(chan UnloadBlock, conc * 4)
	go func() {
		for _, job := range jobs {
			queue <-job
		}
	}()

	// Wait for all errors are handled
	var ew sync.WaitGroup

	// Collecting errors
	errs := make(chan error, conc * 4)
	em := map[string]bool {}
	var es string
	go func() {
		for err := range errs {
			if err != nil {
				s := err.Error()
				if _, ok := em[s]; !ok {
					em[s] = true
					es += s + ";"
				}
			}
			ew.Done()
		}
	}()

	// Read and decode blocks
	blocks := make(chan LoadedBlock, conc * 4)
	for i := 0; i < conc; i++ {
		go func() {
			for job := range queue {
				block, err := job.Indexing.Load(job.Order)
				if err == nil {
					block = pred.Interset(block)
					blocks <-LoadedBlock {job.File, job.Order, block}
				} else {
					blocks <-LoadedBlock {}
				}
				ew.Add(1)
				errs <-err
			}
		}()
	}

	// Reorder blocks
	reordereds := make(chan LoadedBlock, conc * 4)
	reorderer := NewBlockReorderer(jobs)
	go func() {
		for block := range blocks {
			reorderer.Push(block)
			for reorderer.Ready() {
				reordereds <-reorderer.Pop()
			}
		}
		for reorderer.Ready() {
			reordereds <-reorderer.Pop()
		}
		if reorderer.Len() > 0 {
			panic("should never happen: reorderer.holdeds > 0")
		}
	}()

	// Wait for all errors are handled
	var sw sync.WaitGroup

	// Async output blocks
	var sbch chan LoadedBlock
	var sech chan error
	if sink.IsByBlock {
		sw.Add(len(jobs))
		sbch, sech = sink.OnBlock()
		go func() {
			for _ = range jobs {
				err := <-sech
				if err != nil {
					errs <-err
				}
				sw.Done()
			}
		}()
	}

	// Output blocks
	wg.Add(len(jobs))
	go func() {
		for block := range reordereds {
			var err error
			if sink.IsByBlock {
				sbch <- block
			} else {
				for i, row := range block.Block {
					err = sink.OnRow(block.File, block.Order, i, row)
					if err != nil {
						break
					}
				}
			}
			if err != nil {
				ew.Add(1)
				errs <-err
			}
			wg.Done()
		}
	}()

	wg.Wait()
	sw.Wait()
	ew.Wait()

	if len(es) != 0 {
		return fmt.Errorf("partially failed: %s", es)
	}
	return err
}

func (self *BlockReorderer) Len() int {
	return self.holdeds.Len()
}

func (self *BlockReorderer) Push(x interface{}) {
	block := x.(LoadedBlock)
	heap.Push(&self.holdeds, block)
}

func (self *BlockReorderer) Ready() bool {
	if self.Len() <= 0 {
		return false
	}
	top := self.holdeds[0]
	expected := self.origin[self.current]
	return top.File == expected.File && top.Order == expected.Order
}

func (self *BlockReorderer) Pop() LoadedBlock {
	self.current += 1
	return heap.Pop(&self.holdeds).(LoadedBlock)
}

func NewBlockReorderer(origin []UnloadBlock) *BlockReorderer {
	self := &BlockReorderer {origin, 0, LoadedBlocks{}}
	heap.Init(&self.holdeds)
	return self
}

type BlockReorderer struct {
	origin []UnloadBlock
	current int
	holdeds LoadedBlocks
}

func (self *LoadedBlocks) Pop() interface{} {
	old := *self
	n := len(old)
	block := old[n - 1]
	*self = old[0: n - 1]
	return block
}

func (self *LoadedBlocks) Push(x interface{}) {
	block := x.(LoadedBlock)
	*self = append(*self, block)
}

func (self LoadedBlocks) Len() int {
	return len(self)
}

func (self LoadedBlocks) Less(i, j int) bool {
	return self[i].File < self[j].File || (self[i].File == self[j].File && self[i].Order < self[j].Order)
}

func (self LoadedBlocks) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type LoadedBlocks []LoadedBlock

type LoadedBlock struct {
	File string
	Order int
	Block Block
}

type ScanSink struct {
	OnRow func(file string, block int, line int, row Row) error
	OnBlock func() (blocks chan LoadedBlock, result chan error)
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

// TODO: Cache freq blocks (LRU, memory control)
type Cache struct {
	files []string
	indexings map[string]*Indexing
}

type UnloadBlock struct {
	File string
	Order int
	Indexing *Indexing
}

func IndexDump(path string, w io.Writer) error {
	index, err := IndexLoad(path)
	for _, entry := range index {
		_, err = w.Write([]byte(fmt.Sprintf("%v %v\n", entry.Ts, entry.Offset)))
		if err != nil {
			break
		}
	}
	return err
}

func (self Predicate) Interset(block Block) Block {
	if len(block) == 0 {
		return nil
	}
	if !self.Lower.IsOpen() {
		lower := sort.Search(len(block), func(i int) bool {
			return block[i].Ts > self.Lower.Ts || (block[i].Ts == self.Lower.Ts && self.Lower.Included)
		})
		if lower < 0 {
			return nil
		}
		if lower != 0 {
			block = block[lower:]
		}
	}
	if !self.Upper.IsOpen() {
		upper := sort.Search(len(block), func(i int) bool {
			return block[i].Ts > self.Upper.Ts || (block[i].Ts == self.Upper.Ts && !self.Upper.Included)
		})
		if upper >= 0 {
			block = block[0: upper]
		}
	}
	return block
}

type Predicate struct {
	Lower TimestampBound
	Upper TimestampBound
}
