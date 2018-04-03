package main

import "testing"
import "github.com/stretchr/testify/assert"

var testEvent = Event{
	id:        123,
	timestamp: 1522695669429737353,
	account:   789,
	field1:    "one",
	field2:    "two",
	field3:    "three",
	field4:    "four",
	field5:    "five",
}
var testString = "123;1522695669429737353;789;one;two;three;four;five"

func TestMarshall(t *testing.T) {
	e := marshall(testEvent)
	assert.Equal(t, testString, string(e))
}

func TestUnmarshall(t *testing.T) {
	e, err := unmarshall(testString)
	assert.NoError(t, err)
	assert.Equal(t, testEvent, e)
}
