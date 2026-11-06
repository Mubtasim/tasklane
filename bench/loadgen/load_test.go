package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueNameRoundRobin(t *testing.T) {
	require.Equal(t, "bench", queueName(Config{Queue: "bench", Queues: 1}, 7))
	c := Config{Queue: "q", Queues: 3}
	require.Equal(t, []string{"q-0000", "q-0001", "q-0002", "q-0000"},
		[]string{queueName(c, 0), queueName(c, 1), queueName(c, 2), queueName(c, 3)})
}
