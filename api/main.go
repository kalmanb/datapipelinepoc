package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kalmanb/datapipelinepoc/kafka"
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

func process(lines []string, k *kafkaConfig) error {
	fmt.Printf("Processing %d lines\n", len(lines))
	t := time.Now()
	var msgs []kafka.ProducerMessage
	for _, line := range lines {
		e, err := marshallEvent(line)
		if err != nil {
			return err
		}
		ke, err := newKafkaEvent(&e)
		if err != nil {
			return err
		}
		msgs = append(msgs, ke)
	}
	err := k.producer.Send(k.topicInstance, msgs)
	fmt.Printf("Time take to send to kafka %d millis\n", time.Now().Sub(t).Nanoseconds()/1000000)
	return err
}

func marshallEvent(line string) (Event, error) {
	splits := strings.Split(strings.TrimSuffix(line, "\n"), ";")
	if len(splits) < 8 {
		fmt.Printf("%d\n", len(splits))
		return Event{}, errors.New("not enough fields")
	}

	id, err := strconv.ParseUint(splits[0], 10, 32)
	if err != nil {
		return Event{}, nil
	}

	nanos, err := strconv.ParseInt(splits[1], 10, 64)
	if err != nil {
		return Event{}, nil
	}
	timestamp, err := ptypes.TimestampProto(time.Unix(0, nanos))
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
		Id:        uint32(id),
		Timestamp: timestamp,
		Account:   uint32(account),
		Amount:    int64(amount),
		Field1:    splits[4],
		Field2:    splits[5],
		Field3:    splits[6],
		Field4:    splits[7],
		Field5:    splits[8],
	}, nil
}
