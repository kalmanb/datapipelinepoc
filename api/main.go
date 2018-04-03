package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", "localhost:8888")
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	fmt.Printf("Server Started\n")
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Printf("Connection Opened\n")
	b := make([]byte, 1)
	eol := false
	for {
		var lines []string
		var line bytes.Buffer
		for {
			_, err := conn.Read(b)
			if err != nil {
				log.Fatal(err)
			}
			if b[0] == '\n' {
				if eol == true {
					break
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
		process(lines)
	}

}

func process(lines []string) error {
	fmt.Printf("Processing %d lines\n", len(lines))
	for _, line := range lines {
		fmt.Print("bbb")
		fmt.Println(line)
	}
	return nil
}
