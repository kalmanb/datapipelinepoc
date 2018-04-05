package kafka

import "github.com/stretchr/testify/mock"

type MockProducer struct {
	mock.Mock
	Producer
}

func (m *MockProducer) Send(t string, msgs []ProducerMessage) error {
	args := m.Called(t, msgs)
	return args.Error(0)
}

func (m *MockProducer) Close() {
	m.Called()
}

type MockConsumer struct {
	mock.Mock
	Consumer
}

func (m *MockConsumer) Messages() <-chan *ConsumerMessage {
	return m.Called().Get(0).(chan *ConsumerMessage)
}

func (m *MockConsumer) MarkOffset(msg *ConsumerMessage) {
	m.Called(msg)
}

func (m *MockConsumer) MarkLatestOffset(topic string) {
	m.Called(topic)
}

func (m *MockConsumer) Close() {
	m.Called()
}
