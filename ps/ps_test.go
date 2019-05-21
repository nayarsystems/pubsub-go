package ps_test

import (
	"testing"
	"time"

	"github.com/nayarsystems/pubsub-go/ps"
	"github.com/stretchr/testify/assert"
)

func TestSubscribeSimple(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 1, n)

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
}

func TestGetWithoutTimeout(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	t0 := time.Now()
	msg := sub.Get(0)
	t1 := time.Now()

	assert.Nil(t, msg)
	assert.True(t, t1.Sub(t0) < time.Millisecond)
}

func TestGetWithTimeout(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	t0 := time.Now()
	msg := sub.Get(3 * time.Millisecond)
	t1 := time.Now()

	assert.Nil(t, msg)
	assert.True(t, t1.Sub(t0) >= 3*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 4*time.Millisecond)
}

func TestGetBlocking(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	go func() {
		time.Sleep(5 * time.Millisecond)
		ps.Publish(&ps.Msg{To: "a", Data: "b"})
	}()

	t0 := time.Now()
	msg := sub.Get(-1)
	t1 := time.Now()

	assert.Equal(t, msg.To, "a")
	assert.Equal(t, msg.Data, "b")
	assert.True(t, t1.Sub(t0) >= 5*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 6*time.Millisecond)
}

func TestUnsubscribeAll(t *testing.T) {
	sub := ps.NewSubscriber(10, "a")

	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 0, n)

	msg := sub.Get(0)
	assert.Nil(t, msg)
}

func TestUnsubscribe(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a", "b", "c")
	sub.Unsubscribe("a", "b")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "b", Data: "b data"})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "c", Data: "c data"})
	assert.Equal(t, 1, n)

	msg := sub.Get(0)
	assert.Equal(t, "c", msg.To)
	assert.Equal(t, "c data", msg.Data)
	msg = sub.Get(0)
	assert.Nil(t, msg)
	msg = sub.Get(0)
	assert.Nil(t, msg)
}

func TestSubscribeToMultipleTopics(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a", "b")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)
	n = ps.Publish(&ps.Msg{To: "b", Data: "b data"})
	assert.Equal(t, 1, n)

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	msg = sub.Get(0)
	assert.Equal(t, "b", msg.To)
	assert.Equal(t, "b data", msg.Data)
}

func TestMultipleSubscribersToSameTopic(t *testing.T) {
	ps.UnsubscribeAll()

	sub1 := ps.NewSubscriber(10, "a")
	sub2 := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 2, n)

	msg := sub1.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)

	msg = sub2.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
}
