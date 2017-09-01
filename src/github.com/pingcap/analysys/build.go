package analysys

import (
	"bytes"
	"bufio"
	"errors"
	"io"
	"path/filepath"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func FolderBuild(in, out string, compress string, gran, align int, conc int) error {
	in, err := filepath.Abs(in)
	if err != nil {
		return err
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}

	ins := make([]string, 0)
	outs := make([]string, 0)

	err = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == in || strings.HasSuffix(path, IndexFileSuffix) {
			return nil
		}

		output := out + path[len(in):]
		fd, err := os.Open(output)
		if !os.IsNotExist(err) {
			if err == nil {
				fd.Close()
			}
			return nil
		}
		fi, err := os.Open(output + IndexFileSuffix)
		if !os.IsNotExist(err) {
			if err == nil {
				fi.Close()
			}
			return nil
		}

		ins = append(ins, path)
		outs = append(outs, output)
		return nil
	})
	if err != nil {
		return err
	}

	return FilesBuild(ins, outs, compress, gran, align, conc)
}

func FilesBuild(ins, outs []string, compress string, gran, align int, conc int) error {
	// Jobs todo
	type Job struct {
		In string
		Out string
	}
	files := make(chan Job, conc)
	go func() {
		for i, _ := range ins {
			files <-Job {ins[i], outs[i]}
		}
	}()

	// Files loading
	type Loaded struct {
		In string
		Out string
		Rows Rows
		Err error
	}
	loadeds := make(chan Loaded, conc)
	for i := 0; i < conc; i++ {
		go func() {
			for job := range files {
				rows, err := OriginLoad(job.In)
				loadeds <-Loaded {job.In, job.Out, rows, err}
			}
		}()
	}

	// Sort rows of one file
	sorteds := make(chan Loaded, conc)
	for i := 0; i < conc; i++ {
		go func() {
			for job := range loadeds {
				if job.Err == nil {
					sort.Sort(job.Rows)
				}
				sorteds <- job
			}
		}()
	}

	// Write sorted data file and index file
	errs := make(chan error, conc)
	for i := 0; i < conc; i++ {
		go func() {
			for job := range sorteds {
				if job.Err != nil {
					errs <- job.Err
					continue
				}
				index, err := PartWrite(job.Rows, job.Out, compress, gran, align)
				if err == nil {
					err = index.Write(job.Out + IndexFileSuffix)
				}
				errs <- err
			}
		}()
	}

	// Collecting errors
	var es string
	for _ = range ins {
		eg := <-errs
		if eg != nil {
			es += eg.Error() + ";"
		}
	}
	if len(es) != 0 {
		return fmt.Errorf("partially failed: %s", es)
	}
	return nil
}

func PartBuild(in, out string, compress string, gran, align int) error {
	rows, err := OriginLoad(in)
	if err != nil {
		return err
	}

	sort.Sort(rows)

	index, err := PartWrite(rows, out, compress, gran, align)
	if err != nil {
		return err
	}
	return index.Write(out + IndexFileSuffix)
}

func OriginLoad(path string) (Rows, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rows := make(Rows, 0)

	r := bufio.NewReader(f)
	for {
		line, prefix, err := r.ReadLine()
		if err != nil {
			if err != io.EOF {
				return nil, err
			} else {
				return rows, nil
			}
		}
		if prefix {
			return nil, errors.New("line too long")
		}

		row, err := OriginParse(line)
		if err != nil {
			return nil, err
		}
		rows.Add(row)
	}
	return rows, nil
}

func OriginParse(line []byte) (row Row, err error) {
	i := bytes.Index(line, PropsBeginMark)
	if i < 0 {
		err = errors.New("props begin mark not found: " + string(line))
		return
	}
	j := bytes.LastIndex(line, PropsEndMark)
	if j < 0 {
		err = errors.New("props end mark not found: " + string(line))
		return
	}

	props := make([]byte, j + 1 - i)
	copy(props, line[i: j + 1])
	line = line[0: i]

	fields := bytes.Fields(line)
	if len(fields) != 4 {
		err = errors.New("field number not matched: " + string(line))
		return
	}

	id, err := strconv.ParseUint(string(fields[0]), 10, 32)
	if err != nil {
		return
	}
	ts, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return
	}
	event, err := strconv.ParseUint(string(fields[2]), 10, 16)
	if err != nil {
		return
	}

	row.Id = UserId(id)
	row.Ts = Timestamp(ts)
	row.Event = EventId(event)
	row.Props = props
	return row, err
}

var (
	PropsBeginMark = []byte("{")
	PropsEndMark   = []byte("}")
)
