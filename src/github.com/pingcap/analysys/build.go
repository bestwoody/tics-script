package analysys

import (
	"bytes"
	"errors"
	"path/filepath"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"github.com/pingcap/analysys/tools"
)

func FolderBuild(in, out string, compress string, gran, align int, process int) error {
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

		combined := out + path[len(in):]
		fd, err := os.Open(combined)
		if !os.IsNotExist(err) {
			if err == nil {
				fd.Close()
			}
			return nil
		}
		fi, err := os.Open(combined + IndexFileSuffix)
		if !os.IsNotExist(err) {
			if err == nil {
				fi.Close()
			}
			return nil
		}

		ins = append(ins, path)
		outs = append(outs, combined)
		return nil
	})
	if err != nil {
		return err
	}

	type Job struct {
		In string
		Out string
	}
	queue := make(chan Job, process)
	errs := make(chan error, process)
	go func() {
		for i, _ := range ins {
			queue <-Job {ins[i], outs[i]}
		}
	}()
	for i := 0; i < process; i++ {
		go func() {
			for job := range queue {
				err := PartBuild(job.In, job.Out, compress, gran, align)
				errs <-err
			}
		}()
	}
	es := ""
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
	return index.Write(out + IndexFileSuffix);
}

func OriginLoad(path string) (Rows, error) {
	rows := make(Rows, 0)
	err := tools.IterLines(path, BufferSizeRead, func(line []byte) error {
		row, err := OriginParse(line)
		rows.Add(row)
		return err
	})
	return rows, err
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

	row.Id = uint32(id)
	row.Ts = Timestamp(ts / 1000)
	row.Event = uint16(event)
	row.Props = props
	return row, err
}

var (
	PropsBeginMark = []byte("{")
	PropsEndMark   = []byte("}")
)
