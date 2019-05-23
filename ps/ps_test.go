package ps_test

import (
	"fmt"
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
	assert.Equal(t, false, msg.Old)
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
	assert.Equal(t, false, msg.Old)
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
	assert.Equal(t, false, msg.Old)
	msg = sub.Get(0)
	assert.Equal(t, "b", msg.To)
	assert.Equal(t, "b data", msg.Data)
	assert.Equal(t, false, msg.Old)
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
	assert.Equal(t, false, msg.Old)

	msg = sub2.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestUnsubscribeAllOnSubscriber(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a", "b")
	sub.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "b", Data: "b data"})
	assert.Equal(t, 0, n)

	msg := sub.Get(0)
	assert.Nil(t, msg)
	msg = sub.Get(0)
	assert.Nil(t, msg)
}

func TestMultipleSubscribersToSameTopicAndOneUnsubscribes(t *testing.T) {
	ps.UnsubscribeAll()

	sub1 := ps.NewSubscriber(10, "a")
	sub2 := ps.NewSubscriber(10, "a")

	sub2.Unsubscribe("a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)

	msg := sub1.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub2.Get(0)
	assert.Nil(t, msg)
}

func TestMultipleSubscribersToSameTopicAndOneUnsubscribesWithUnsubscribeAll(t *testing.T) {
	ps.UnsubscribeAll()

	sub1 := ps.NewSubscriber(10, "a")
	sub2 := ps.NewSubscriber(10, "a")

	sub2.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)

	msg := sub1.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)

	msg = sub2.Get(0)
	assert.Nil(t, msg)
}

func TestSticky(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestStickyGetLastPublishedValue(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a1"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "a", Data: "a2"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a2", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestNormalValueClearsSticky(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a1"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "a", Data: "a2"})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(0)
	assert.Nil(t, msg)
}

func TestStickyNotLostAfterUnsusbcribing(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 1, n)

	sub.Unsubscribe("a")

	sub = ps.NewSubscriber(10, "a")

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestCanCheckOverflowOnSubscriber(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(1, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 1, n)

	assert.Equal(t, uint32(0), sub.Overflow())

	n = ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 0, n)

	assert.Equal(t, uint32(1), sub.Overflow())
}

func TestUnsubscribeFromNonSubscribedPath(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(1, "a")
	sub.Unsubscribe("b")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 1, n)

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestSendToParentTopics(t *testing.T) {
	ps.UnsubscribeAll()

	subA := ps.NewSubscriber(1, "a")
	subAB := ps.NewSubscriber(1, "a.b")

	n := ps.Publish(&ps.Msg{To: "a.b.c", Data: "whatever"})
	assert.Equal(t, 2, n)

	msg := subA.Get(0)
	assert.Equal(t, "a.b.c", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = subAB.Get(0)
	assert.Equal(t, "a.b.c", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestHiddenFlagDoesntCountAsDelivered(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(1, "a h")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 0, n)

	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestGetWaitingMessagesNotRead(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a")

	for i := 0; i < 5; i++ {
		n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
		assert.Equal(t, 1, n)
	}

	sub.Get(0)

	assert.Equal(t, 4, sub.Waiting())
}

func TestDontReceiveStickyFromChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(0)
	assert.Nil(t, msg)
}

func TestFlagForNotReceivingStickyFromTopicOrItsChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subAB := ps.NewSubscriber(10, "a.b s")
	msg := subAB.Get(0)
	assert.Nil(t, msg)

	subA := ps.NewSubscriber(10, "a s")
	msg = subA.Get(0)
	assert.Nil(t, msg)
}

func TestFlagForReceivingStickyFromTopicAndItsChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subAB := ps.NewSubscriber(10, "a.b S")
	msg := subAB.Get(0)
	assert.Equal(t, "a.b", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, true, msg.Old)

	subA := ps.NewSubscriber(10, "a S")
	msg = subA.Get(0)
	assert.Equal(t, "a.b", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestNormalMessageDoesntClearStickyOnParents(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	n = ps.Publish(&ps.Msg{To: "a.b", Data: "a.b data"})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")
	msg := sub.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, true, msg.Old)

	msg = sub.Get(0)
	assert.Nil(t, msg)
}

func TestStickyOnParentIsNotReceivedByChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subA := ps.NewSubscriber(10, "a")
	msg := subA.Get(0)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, true, msg.Old)

	subAB := ps.NewSubscriber(10, "a.b")
	msg = subAB.Get(0)
	assert.Nil(t, msg)
}

func TestStickyOnSiblingIsNotReceived(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "b")
	msg := sub.Get(0)
	assert.Nil(t, msg)
}

func TestStickyOnSiblingChildrenIsNotReceived(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "c")
	msg := sub.Get(0)
	assert.Nil(t, msg)
}

func TestCall(t *testing.T) {
	ps.UnsubscribeAll()

	ready := make(chan bool)

	go func() {
		sub := ps.NewSubscriber(10, "a")
		ready <- true
		for {
			msg := sub.Get(time.Second)
			if msg == nil {
				break
			}
			msg.Answer("hello "+msg.Data.(string), nil)
		}
	}()

	<-ready

	result, err := ps.Call(&ps.Msg{To: "a", Data: "Peter"}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "hello Peter", result)
}

func TestCallReturnError(t *testing.T) {
	ps.UnsubscribeAll()

	ready := make(chan bool)

	go func() {
		sub := ps.NewSubscriber(10, "a")
		ready <- true
		for {
			msg := sub.Get(time.Second)
			if msg == nil {
				break
			}
			msg.Answer(nil, fmt.Errorf("error %s", msg.Data))
		}
	}()

	<-ready

	result, err := ps.Call(&ps.Msg{To: "a", Data: "Peter"}, time.Second)
	assert.Nil(t, result)
	assert.Equal(t, fmt.Errorf("error Peter"), err)
}

func TestCallTimeoutReturnsErrTimeout(t *testing.T) {
	ps.UnsubscribeAll()

	result, err := ps.Call(&ps.Msg{To: "a", Data: "Peter"}, time.Nanosecond)
	assert.Nil(t, result)
	assert.IsType(t, &ps.ErrTimeout{}, err)
}

func TestCallTimeout(t *testing.T) {
	ps.UnsubscribeAll()

	t0 := time.Now()
	result, err := ps.Call(&ps.Msg{To: "a", Data: "Peter"}, time.Millisecond*4)
	t1 := time.Now()

	assert.Nil(t, result)
	assert.IsType(t, &ps.ErrTimeout{}, err)
	assert.True(t, t1.Sub(t0) >= 4*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 5*time.Millisecond)
}

func TestCallBlocking(t *testing.T) {
	ps.UnsubscribeAll()

	ready := make(chan bool)

	go func() {
		sub := ps.NewSubscriber(1, "a")
		ready <- true
		msg := sub.Get(time.Second)
		time.Sleep(3 * time.Millisecond)
		msg.Answer("b", nil)
	}()

	<-ready

	t0 := time.Now()
	result, err := ps.Call(&ps.Msg{To: "a", Data: "Peter"}, -1)
	t1 := time.Now()

	assert.Equal(t, "b", result)
	assert.NoError(t, err)
	assert.True(t, t1.Sub(t0) >= 3*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 4*time.Millisecond)
}
