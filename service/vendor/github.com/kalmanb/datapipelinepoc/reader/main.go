package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

var batchSize = 10
var conn net.Conn

func main() {
	f, err := os.OpenFile("../db.log", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	conn, err = net.Dial("tcp", "localhost:8888")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	tail(f, process)
}

func process(lines []string) error {
	var buf bytes.Buffer
	for _, line := range lines {
		event, err := unmarshall(line)
		if err != nil {
			return err
		}
		b := marshall(event)
		buf.Write(b)
		buf.WriteString("\n")
	}
	// Send to API
	buf.WriteString("\n")
	if err := send(buf.Bytes()); err != nil {
		return err
	}

	fmt.Printf("Processed %d lines\n", len(lines))
	return nil
}

func send(b []byte) error {
	_, err := conn.Write(b)
	if err != nil {
		return err
	}
	conn.Read(nil)
	return nil
}

func unmarshall(line string) (Event, error) {
	splits := strings.Split(strings.TrimSuffix(line, "\n"), ";")
	if len(splits) < 8 {
		fmt.Printf("%d\n", len(splits))
		return Event{}, errors.New("not enough fields")
	}

	id, err := strconv.ParseUint(splits[0], 10, 32)
	if err != nil {
		return Event{}, nil
	}
	timestamp, err := strconv.ParseUint(splits[1], 10, 64)
	if err != nil {
		return Event{}, nil
	}
	account, err := strconv.ParseUint(splits[2], 10, 32)
	if err != nil {
		return Event{}, nil
	}
	amount, err := strconv.ParseInt(splits[3], 10, 64)
	if err != nil {
		return Event{}, nil
	}
	return Event{
		id:        uint32(id),
		timestamp: timestamp,
		account:   uint32(account),
		amount:    int64(amount),
		field1:    splits[4],
		field2:    splits[5],
		field3:    splits[6],
		field4:    splits[7],
		field5:    splits[8],
	}, nil
}

func marshall(e Event) []byte {
	s := fmt.Sprintf("%d;%d;%d;%d;%s;%s;%s;%s;%s", e.id, e.timestamp, e.account, e.amount, e.field1, e.field2, e.field3, e.field4, e.field5)
	return []byte(s)
}

type Event struct {
	id        uint32
	timestamp uint64
	account   uint32
	amount    int64
	field1    string
	field2    string
	field3    string
	field4    string
	field5    string
}
