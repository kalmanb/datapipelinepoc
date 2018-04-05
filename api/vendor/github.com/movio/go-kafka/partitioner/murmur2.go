package partitioner

import "github.com/Shopify/sarama"

// Murmur2Partitioner is a matching implemention for the current (10.1) kafka partitioner. It will generate the same partition ID's a the default Kafka client and hence can be as a replacement with correct inter op
type Murmur2Partitioner struct {
	random sarama.Partitioner
}

// NewMurmur2Partitioner creates a partitioner
func NewMurmur2Partitioner(topic string) sarama.Partitioner {
	p := new(Murmur2Partitioner)
	p.random = sarama.NewRandomPartitioner(topic)
	return p
}

// Called by Samza to get the partition for a message.
// Will return a random partition if the message key is nil
func (p *Murmur2Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	return Murmur2Partition(bytes, numPartitions), nil
}

// RequiresConsistency is always true for this implemention
func (p *Murmur2Partitioner) RequiresConsistency() bool {
	return true
}

func Murmur2Partition(bytes []byte, numPartitions int32) int32 {
	hash := MurmurHash2(bytes)
	partition := positive(hash) % numPartitions
	return partition
}

// From https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L728
func positive(v int32) int32 {
	return v & 0x7fffffff
}

// The original MurmurHash2 32-bit algorithm by Austin Appleby.
// Taken from https://github.com/aviddiviner/go-murmur by David Irvine
// Adapted to match the behavior of the Java Kafka Client
func MurmurHash2(data []byte) (h int32) {
	const (
		M = 0x5bd1e995
		R = 24
		// From https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L342
		seed = int32(-1756908916)
	)

	var k int32

	h = seed ^ int32(len(data))

	// Mix 4 bytes at a time into the hash
	for l := len(data); l >= 4; l -= 4 {
		k = int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		k *= M
		k ^= int32(uint32(k) >> R) // To match Kafka Impl
		k *= M
		h *= M
		h ^= k
		data = data[4:]
	}

	// Handle the last few bytes of the input array
	switch len(data) {
	case 3:
		h ^= int32(data[2]) << 16
		fallthrough
	case 2:
		h ^= int32(data[1]) << 8
		fallthrough
	case 1:
		h ^= int32(data[0])
		h *= M
	}

	// Do a few final mixes of the hash to ensure the last few bytes are well incorporated
	h ^= int32(uint32(h) >> 13)
	h *= M
	h ^= int32(uint32(h) >> 15)

	return
}
