package main

import (
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/kalmanb/datapipelinepoc/kafka"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var batchSize = 20

func main() {
	consumer := kafka.MustSetupConsumer(kafka.ConsumerConfig{
		Brokers:       "localhost:9092",
		ConsumerGroup: "service_sub",
		Topics:        "host_events",
	})
	defer consumer.Close()

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "events"
	cluster.Consistency = gocql.Quorum
	cql, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer cql.Close()

	go consumeKafkaQ(consumer, cql)
}

func processBatch(msgs []*kafka.ConsumerMessage, cql *gocql.Session) error {
	for _, msg := range msgs {
		var ke KafkaEvent
		if err := proto.Unmarshal(msg.Value, &ke); err != nil {
			err := errors.Wrap(err, "could not unmarshal item proto msg")
			log.WithFields(log.Fields{
				"protobufBinary": string(msg.Value),
			}).Error(err)
			return err
		}

		if err := persist(ke.Data, cql); err != nil {
			return err
		}
	}
	return nil
}

func persist(e *Event, cql *gocql.Session) error {
	if err := cql.Query(`INSERT INTO event (account, id, timestamp, amount, field1, field2, field3, field4, field5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		strconv.FormatUint(uint64(e.Account), 10),
		e.Id,
		e.Timestamp.GetNanos(), // FIXME - this is wrong
		e.Amount,
		e.Field1,
		e.Field2,
		e.Field3,
		e.Field4,
		e.Field5).Exec(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func consumeKafkaQ(consumer kafka.Consumer, cql *gocql.Session) {
foreverLoop:
	for {
		msgs := []*kafka.ConsumerMessage{}
		//loop until we have BatchSize # of messages in our batch (retrieve messages from kafka q)
		// fmt.Println("waiting")
	batchLoop:
		for len(msgs) < batchSize {
			select {
			case msg, ok := <-consumer.Messages():
				if !ok {
					if len(msgs) > 0 {
						break batchLoop
					} else {
						log.Error("Consumer msg channel closed, exiting")
						break foreverLoop
					}
				}
				msgs = append(msgs, msg)
				// fmt.Println("got some")
			case <-time.After(100 * time.Millisecond):
				// if we don't get enough messages to fill a batch, timeout after BatchTimeout seconds, then process current messages
				break batchLoop
			}
		}
		// fmt.Println("done")

		//if we have messages to process
		if len(msgs) > 0 {
			//loop until the messages in the batch were successfully sent
			err := processBatch(msgs, cql)
			if err != nil {
				// Counld retry here?
				log.WithError(err).Fatalln("Counld not process messages")
			}

			// Mark offsets for each partition - starting with last messages first
			marked := make(map[int32]struct{})
			for i := len(msgs) - 1; i > 0; i-- {
				if marked[msgs[i].Partition] != struct{}{} {
					consumer.MarkOffset(msgs[i])
					marked[msgs[i].Partition] = struct{}{}
				}
			}

			log.Infof("Processed %d items", len(msgs))
		}
	}
}
