package kafka

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/movio/go-kafka/partitioner"
)

// ProducerMessage is to be implemented by all messages to be send through this library
type ProducerMessage interface {
	// getKey returns the key for the kafka message, this can be set for each message type separately
	GetKey() string
}

type Serializer func(ProducerMessage) ([]byte, error)

type Producer interface {
	Send(topic string, values []ProducerMessage) error
	SendBytes(topic string, values [][]byte) error
	Close()
}

type producerImp struct {
	saramaProducer sarama.SyncProducer
	serializer     Serializer
}

// Send can be used to send/produce new messages to a topic
func (p *producerImp) Send(topic string, values []ProducerMessage) error {
	var msgs []*sarama.ProducerMessage
	for _, value := range values {
		bytes, err := p.serializer(value)
		if err != nil {
			return err
		}

		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(value.GetKey()),
			Value: sarama.ByteEncoder(bytes),
		})
	}

	if len(msgs) > 0 {
		err := p.saramaProducer.SendMessages(msgs)
		return err
	}
	return errors.New("no messages were sent")
}

//This is used for tests
func (p *producerImp) SendBytes(topic string, values [][]byte) error {
	var msgs []*sarama.ProducerMessage
	for _, bytes := range values {
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(bytes),
		})
	}

	if len(msgs) > 0 {
		err := p.saramaProducer.SendMessages(msgs)
		return err
	}
	return errors.New("No messages were sent")
}

// Close can be used to cleanly shut down the producer
func (p *producerImp) Close() {
	if err := p.saramaProducer.Close(); err != nil {
		log.Errorln(err)
	}
}

// mustSetupProducer creates a producer for the kafka queues on brokers
// brokers is a comma separated list of kafka nodes with host and ip, eg "172.0.0.1:9092,172.0.0.2:9092"
func MustSetupProducer(brokers string) Producer {
	defaultSerializer := func(msg ProducerMessage) ([]byte, error) {
		return json.Marshal(msg)
	}

	return MustSetupProducerWithSerializer(brokers, defaultSerializer)
}

func MustSetupProducerWithSerializer(brokers string, serializer Serializer) Producer {
	producer, err := SetupProducer(brokers, serializer)
	if err != nil {
		log.Fatalln(err)
	}
	return producer
}

// mustSetupProducer creates a producer for the kafka queues on brokers
// brokers is a comma separated list of kafka nodes with host and ip, eg "172.0.0.1:9092,172.0.0.2:9092"
func SetupProducer(brokers string, serializer Serializer) (Producer, error) {
	brokerList := strings.Split(brokers, ",")
	if len(brokerList) == 0 {
		return nil, errors.New("The Kafka producer requires at least 1 broker to resolve topics.")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.TLS.Enable = false
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	saramaConfig.Producer.Retry.Max = 10
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true
	// saramaConfig.Producer.Flush.Frequency = 50 * time.Millisecond
	saramaConfig.Producer.Partitioner = partitioner.NewMurmur2Partitioner

	saramaProducer, err := sarama.NewSyncProducer(brokerList, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &producerImp{saramaProducer: saramaProducer, serializer: serializer}, nil
}
