package partitioner

import "testing"

var tests = []struct {
	key         []byte
	expectedPos int32
	expected2   int32
	expected3   int32
	expected100 int32
	expected999 int32
}{
	{[]byte(""), 275646681, 1, 0, 81, 603},
	{[]byte("⌘"), 39915425, 1, 2, 25, 380},
	{[]byte("oh no"), 939168436, 0, 1, 36, 544},
	{[]byte("c03a3475-3ed6-4ed1-8ae5-1c432da43e73"), 376769867, 1, 2, 67, 14},
}

func TestHasing(t *testing.T) {
	for _, i := range tests {
		if v := positive(MurmurHash2(i.key)); v != i.expectedPos {
			t.Errorf("Positive, expected: %v, got: %v\n", i.expectedPos, v)
		}
		if v := Murmur2Partition(i.key, 2); v != i.expected2 {
			t.Errorf("2 patitions, expected: %v, got: %v\n", v, i.expected2)
		}
		if v := Murmur2Partition(i.key, 3); v != i.expected3 {
			t.Errorf("3 patitions, expected: %v, got: %v\n", v, i.expected3)
		}
		if v := Murmur2Partition(i.key, 100); v != i.expected100 {
			t.Errorf("100 patitions, expected: %v, got: %v\n", v, i.expected100)
		}
		if v := Murmur2Partition(i.key, 999); v != i.expected999 {
			t.Errorf("999 patitions, expected: %v, got: %v\n", v, i.expected999)
		}
	}
}
