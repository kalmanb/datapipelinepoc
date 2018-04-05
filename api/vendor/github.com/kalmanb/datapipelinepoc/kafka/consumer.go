package kafka

import (
	"errors"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

const OffsetOldest = sarama.OffsetOldest
const OffsetNewest = sarama.OffsetNewest

// ConsumerConfig contains the different configuration options for the consumer
type ConsumerConfig struct {

	// Brokers is a mandatory comma separated list of kafka nodes with host and ip, eg "172.0.0.1:9092,172.0.0.2:9092"
	Brokers string
	// ConsumerGroup is a mandatory name for the consumer group that is being used by Kafka
	ConsumerGroup string
	// Topics is a mandatory comma separated list of topics from which to consume the messages
	Topics string

	// ConsumerTimeout is the amount of seconds before a consumer poll times out, default is 100
	SessionTimeout int

	// BufferSize is the size of the channel on which the kafka messages are loaded, default is 0 (no buffering)
	BufferSize int

	// InitialOffset is used if no offset was previously committed.
	// Should be OffsetNewest or OffsetOldest. Defaults to OffsetOldest.
	InitialOffset int64

	// OnError is an optional error handler. By default all errors are logged, but ignored
	OnError func(error)

	// OnNotification is an optional notification handler. By default all notifications are logged, but ignored
	OnNotification func(*cluster.Notification)

	// MetricsRegistry to define metrics into.
	// If you want to disable metrics gathering, set "metrics.UseNilMetrics" to "true"
	MetricsRegistry metrics.Registry

	ParitionStrategy cluster.Strategy
}

// ConsumerMessage is
type ConsumerMessage struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Timestamp  time.Time
}

type Consumer interface {

	// Messages returns the message channel for the consumer
	// it is the channel where new kafka massages from the topic will go to
	Messages() <-chan *ConsumerMessage

	// MarkOffset can be used to mark the last processed message
	MarkOffset(msg *ConsumerMessage)

	// MarkLatestOffset can be used to update the topic offsets for the consumer group to the latest offsets
	MarkLatestOffset(topic string)

	// Close can be used to cleanly shut down the consumer
	Close()
}

type consumerImp struct {
	config          ConsumerConfig
	clusterConsumer *cluster.Consumer
	messages        <-chan *ConsumerMessage
}

func (c *consumerImp) Messages() <-chan *ConsumerMessage {
	return c.messages
}

func (c *consumerImp) MarkOffset(msg *ConsumerMessage) {
	c.clusterConsumer.MarkOffset(&sarama.ConsumerMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}, "")
}

func (c *consumerImp) MarkLatestOffset(topic string) {
	brokerList := strings.Split(c.config.Brokers, ",")
	for k, v := range brokerList {
		brokerList[k] = strings.TrimSpace(v)
	}

	client, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	for _, partition := range partitions {
		offset, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
		c.clusterConsumer.MarkPartitionOffset(topic, partition, offset, "")
		log.Infof("Marked latest offset for [%s], partition[%d] -> [%d]", topic, partition, offset)
	}
}

func (c *consumerImp) Close() {
	if err := c.clusterConsumer.Close(); err != nil {
		log.Error(err)
	}
}

// MustSetupConsumer creates a consumer for the config.InputTopics on config.InputBrokers
func MustSetupConsumer(config ConsumerConfig) Consumer {
	consumer, err := SetupConsumer(config)
	if err != nil {
		log.Fatal(err)
	}

	return consumer
}

// MustSetupConsumer creates a consumer for the config.InputTopics on config.InputBrokers
func SetupConsumer(config ConsumerConfig) (Consumer, error) {
	// default error handler
	if config.OnError == nil {
		config.OnError = func(err error) {
			log.Error(err)
		}
	}

	if config.OnNotification == nil {
		config.OnNotification = func(notification *cluster.Notification) {
			log.Infof("(Rebalance) Notification: [%+v]", notification)
		}
	}

	brokerList := strings.Split(config.Brokers, ",")
	for k, v := range brokerList {
		brokerList[k] = strings.TrimSpace(v)
	}
	if len(brokerList) == 0 {
		return nil, errors.New("The Kafka consumer requires at least 1 broker to resolve topics.")
	}

	topicList := strings.Split(config.Topics, ",")
	for k, v := range topicList {
		topicList[k] = strings.TrimSpace(v)
	}
	if len(topicList) == 0 {
		return nil, errors.New("The Kafka consumer requires at least 1 topic to consume.")
	}

	clusterConfig := cluster.NewConfig()
	clusterConfig.Consumer.Return.Errors = true

	if config.ParitionStrategy == "" {
		clusterConfig.Group.PartitionStrategy = cluster.StrategyRoundRobin
	} else {
		clusterConfig.Group.PartitionStrategy = config.ParitionStrategy
	}

	if config.InitialOffset == 0 {
		clusterConfig.Consumer.Offsets.Initial = OffsetOldest
	} else {
		clusterConfig.Consumer.Offsets.Initial = config.InitialOffset
	}

	if config.SessionTimeout > 0 {
		clusterConfig.Group.Session.Timeout = time.Duration(config.SessionTimeout) * time.Second
	}

	if config.MetricsRegistry != nil {
		clusterConfig.MetricRegistry = metrics.NewPrefixedChildRegistry(config.MetricsRegistry, "kafka-")
	}

	clusterConsumer, err := cluster.NewConsumer(brokerList, config.ConsumerGroup, topicList, clusterConfig)
	if err != nil {
		return nil, err
	}

	// handle messages
	msgs := make(chan *ConsumerMessage, config.BufferSize)
	go func() {
		defer close(msgs)

		for {
			msg, ok := <-clusterConsumer.Messages()
			if !ok {
				log.Infof("Consumer message channel for [%s] closed", config.Topics)
				return
			}

			msgs <- &ConsumerMessage{
				Key:       msg.Key,
				Value:     msg.Value,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
			}
		}
	}()

	// handle errors
	go func() {
		for {
			err, ok := <-clusterConsumer.Errors()
			if !ok {
				log.Infof("Consumer error channel for [%s] closed", config.Topics)
				return
			}
			config.OnError(err)
		}
	}()

	// handle notifications
	go func() {
		for {
			notification, ok := <-clusterConsumer.Notifications()
			if !ok {
				log.Infof("Consumer notifications channel for [%s] closed", config.Topics)
				return
			}
			config.OnNotification(notification)
		}
	}()
	return &consumerImp{config: config, clusterConsumer: clusterConsumer, messages: msgs}, nil
}
