package analysys

import (
	"io"
	"path/filepath"
	"fmt"
	"os"
	"sort"
	"strings"
)

func FolderDump(path string, backlog int, w io.Writer) error {
	ts := Timestamp(0)
	return FolderScan(path, backlog, func(file string, line int, row Row) error {
		if row.Ts < ts {
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

	// TODO: parallel, cache, pipeline

	for _, path := range ins {
		indexing, err := IndexingLoad(path)
		if err != nil {
			return err
		}
		line := 0
		for i, _ := range(indexing.Index) {
			block, err := indexing.Load(i)
			if err != nil {
				return err
			}
			for _, row := range block {
				err = fun(path, line, row)
				if err != nil {
					return err
				}
				line += 1
			}
		}
	}
	return err
}

func PartDump(path string, w io.Writer) error {
	indexing, err := IndexingLoad(path)
	if err != nil {
		return err
	}
	ts := Timestamp(0)
	for i, _ := range(indexing.Index) {
		block, err := indexing.Load(i)
		if err != nil {
			return err
		}
		for j, row := range block {
			s := row.String()
			_, err = w.Write([]byte(s + "\n"))
			if err != nil {
				return err
			}
			if row.Ts < ts {
				return fmt.Errorf("backward timestamp: block: %v row:%v %s", i, j, s)
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
	for i, _ := range(indexing.Index) {
		_, err := indexing.Load(i)
		if err != nil {
			return err
		}
	}
	return nil
}
