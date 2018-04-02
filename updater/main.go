package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

// 5 fields
var fieldLen = 10

func main() {
	f, err := os.OpenFile("../db.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	i := 0
	for true {
		d := time.Duration(10 * rand.Intn(10))
		time.Sleep(d * time.Millisecond)
		writeLine(f, i)
		i++
	}
}

func writeLine(f *os.File, i int) {
	if _, err := f.Write(getLog(i)); err != nil {
		panic(err)
	}

	if err := f.Sync(); err != nil {
		panic(err)
	}
}

func getLog(i int) []byte {
	s := fmt.Sprintf("%d;%d;%s;%s;%s;%s;%s\n", i, time.Now().UnixNano(), randString(fieldLen), randString(fieldLen), randString(fieldLen), randString(fieldLen), randString(fieldLen))
	return []byte(s)
}

var chars = []rune("abcdefghijklmnopqrstuvwxyz")

func randString(l int) string {
	b := make([]rune, l)
	for i := 0; i < l; i++ {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
