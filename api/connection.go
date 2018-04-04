package main

import (
	"bytes"
	fmt "fmt"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	fmt.Printf("Connection Opened\n")
	producer := getProducer("127.0.0.1:9092", "host_events")
	b := make([]byte, 1)
	eol := false
	for {
		var lines []string
		var line bytes.Buffer
	batchLoop:
		for {
			_, err := conn.Read(b)
			if err != nil {
				log.Fatal(err)
			}
			if b[0] == '\n' {
				if eol == true {
					break batchLoop
				}
				lines = append(lines, line.String())
				eol = true
				line.Reset()
			} else {
				line.Write(b)
				eol = false
			}
		}
		_, err := conn.Write([]byte("OK\n\n"))
		if err != nil {
			log.Fatal(err)
		}
		err = process(lines, producer)
		if err != nil {
			log.Fatal(err)
		}
	}

}
