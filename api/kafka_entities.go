package main

import (
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/kalmanb/datapipelinepoc/kafka"
)

func topicName(topicInstance string) string {
	return topicInstance //fmt.Sprintf("river.transactions.%s", topicInstance)
}

func (m *KafkaEvent) GetKey() string {
	return string(m.Data.Id)
}

func newKafkaEvent(e *Event) (*KafkaEvent, error) {
	generatedTime, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		return nil, err
	}
	return &KafkaEvent{
		Data:             e,
		UtcGeneratedTime: generatedTime,
	}, nil
}

type kafkaConfig struct {
	producer      kafka.Producer
	topicInstance string
}

func getProducer(brokers string, topic string) *kafkaConfig {
	pbSerializer := kafka.Serializer(func(msg kafka.ProducerMessage) ([]byte, error) {
		cMsg := msg.(proto.Message)
		return proto.Marshal(cMsg)
	})
	return newKafkaConfig(kafka.MustSetupProducerWithSerializer(brokers, pbSerializer), topic)
}

func newKafkaConfig(producer kafka.Producer, topicInstance string) *kafkaConfig {
	return &kafkaConfig{
		producer:      producer,
		topicInstance: topicInstance,
	}
}
