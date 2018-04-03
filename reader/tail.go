package main

import (
	"bufio"
	"io"
	"os"
	"time"
)

func tail(f *os.File, processFn func([]string) error) {
	r := bufio.NewReader(f)
	info, err := f.Stat()
	if err != nil {
		panic(err)
	}
	oldSize := info.Size()
	count := 0
	for {
		count = batchSize
		for count >= batchSize {
			count = 0
			var lines []string
			// Assuming lines aren't too big
			for line, prefix, err := r.ReadLine(); err != io.EOF && count < batchSize; line, prefix, err = r.ReadLine() {
				if prefix {
					panic("Line was too ling")
				} else {
					lines = append(lines, string(line))
				}
				count++
			}
			if err = process(lines); err != nil {
				panic(err)
			}
		}
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			panic(err)
		}
		for {
			time.Sleep(100 * time.Millisecond)
			newinfo, err := f.Stat()
			if err != nil {
				panic(err)
			}
			newSize := newinfo.Size()
			if newSize != oldSize {
				if newSize < oldSize {
					f.Seek(0, 0)
				} else {
					f.Seek(pos, io.SeekStart)
				}
				r = bufio.NewReader(f)
				oldSize = newSize
				break
			}
		}
	}
}
