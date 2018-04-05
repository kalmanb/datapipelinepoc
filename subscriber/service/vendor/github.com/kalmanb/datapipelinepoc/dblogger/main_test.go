package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandString(t *testing.T) {
	assert.Equal(t, 10, len(randString(10)))
}
