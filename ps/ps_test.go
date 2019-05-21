package ps_test

import (
	"testing"

	"github.com/nayarsystems/pubsub-go/ps"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeSimple(t *testing.T) {
	sub := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 1, n)

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
}
