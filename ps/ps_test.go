package ps_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nayarsystems/pubsub-go/ps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeSimple(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 1, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
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
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
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

	require.NotNil(t, msg)
	assert.Equal(t, msg.To, "a")
	assert.Equal(t, msg.Data, "b")
	assert.True(t, t1.Sub(t0) >= 5*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestUnsubscribeAll(t *testing.T) {
	sub := ps.NewSubscriber(10, "a")

	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 0, n)

	msg := sub.Get(10 * time.Millisecond)
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

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "c", msg.To)
	assert.Equal(t, "c data", msg.Data)
	assert.Equal(t, false, msg.Old)
	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestSubscribeToMultipleTopics(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a", "b")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)
	n = ps.Publish(&ps.Msg{To: "b", Data: "b data"})
	assert.Equal(t, 1, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)
	msg = sub.Get(time.Second)
	require.NotNil(t, msg)
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

	msg := sub1.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub2.Get(time.Second)
	require.NotNil(t, msg)
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

	msg := sub.Get(time.Millisecond)
	assert.Nil(t, msg)
	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestMultipleSubscribersToSameTopicAndOneUnsubscribes(t *testing.T) {
	ps.UnsubscribeAll()

	sub1 := ps.NewSubscriber(10, "a")
	sub2 := ps.NewSubscriber(10, "a")

	sub2.Unsubscribe("a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)

	msg := sub1.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub2.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestMultipleSubscribersToSameTopicAndOneUnsubscribesWithUnsubscribeAll(t *testing.T) {
	ps.UnsubscribeAll()

	sub1 := ps.NewSubscriber(10, "a")
	sub2 := ps.NewSubscriber(10, "a")

	sub2.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"})
	assert.Equal(t, 1, n)

	msg := sub1.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)

	msg = sub2.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestSticky(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestDontGetStickyFromChildrenTopics(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	n = ps.Publish(&ps.Msg{To: "a.b", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "b", msg.Data)
	assert.Equal(t, true, msg.Old)

	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestWhenGettingStickFromChildrenDontReceiveRepeatedStickyOnTopic(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "c"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	n = ps.Publish(&ps.Msg{To: "a.b", Data: "c"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a S")

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "c", msg.Data)
	assert.Equal(t, true, msg.Old)

	msg = sub.Get(time.Second)
	assert.Equal(t, "a.b", msg.To)
	assert.Equal(t, "c", msg.Data)
	assert.Equal(t, true, msg.Old)

	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestStickyGetLastPublishedValue(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a1"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)
	n = ps.Publish(&ps.Msg{To: "a", Data: "a2"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
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

	msg := sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestStickyNotLostAfterUnsusbcribing(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 1, n)

	sub.Unsubscribe("a")

	sub = ps.NewSubscriber(10, "a")

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
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

	msg := sub.Get(time.Second)
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

	msg := subA.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a.b.c", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = subAB.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a.b.c", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestHiddenFlagDoesntCountAsDelivered(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(1, "a h")

	n := ps.Publish(&ps.Msg{To: "a", Data: "b"})
	assert.Equal(t, 0, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
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

	sub.Get(time.Second)

	assert.Equal(t, 4, sub.Waiting())
}

func TestDontReceiveStickyFromChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "a")

	msg := sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestFlagForNotReceivingStickyFromTopicOrItsChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subAB := ps.NewSubscriber(10, "a.b s")
	msg := subAB.Get(time.Millisecond)
	assert.Nil(t, msg)

	subA := ps.NewSubscriber(10, "a s")
	msg = subA.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestFlagForReceivingStickyFromTopicAndItsChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "whatever"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subAB := ps.NewSubscriber(10, "a.b S")
	msg := subAB.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a.b", msg.To)
	assert.Equal(t, "whatever", msg.Data)
	assert.Equal(t, true, msg.Old)

	subA := ps.NewSubscriber(10, "a S")
	msg = subA.Get(time.Second)
	require.NotNil(t, msg)
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
	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, true, msg.Old)

	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestStickyOnParentIsNotReceivedByChildren(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	subA := ps.NewSubscriber(10, "a")
	msg := subA.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, true, msg.Old)

	subAB := ps.NewSubscriber(10, "a.b")
	msg = subAB.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestStickyOnSiblingIsNotReceived(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "b")
	msg := sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestStickyOnSiblingChildrenIsNotReceived(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a.b", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	sub := ps.NewSubscriber(10, "c")
	msg := sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestCall(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

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

	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "hello Peter", result)
}

func TestCallReturnError(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

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

	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, time.Second)
	assert.Nil(t, result)
	assert.Equal(t, fmt.Errorf("error Peter"), err)
}

func TestCallTimeoutReturnsDeadlineExceeded(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

	ps.NewSubscriber(10, "a")

	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, time.Nanosecond)
	assert.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestCallTimeout(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

	ps.NewSubscriber(1, "a")

	t0 := time.Now()
	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, time.Millisecond*4)
	t1 := time.Now()

	assert.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.True(t, t1.Sub(t0) >= 4*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestCallBlocking(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

	ready := make(chan bool)

	go func() {
		sub := ps.NewSubscriber(1, "a")
		ready <- true
		msg := sub.Get(time.Second)
		require.NotNil(t, msg)
		time.Sleep(3 * time.Millisecond)
		msg.Answer("b", nil)
	}()

	<-ready

	t0 := time.Now()
	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, -1)
	t1 := time.Now()

	assert.Equal(t, "b", result)
	assert.NoError(t, err)
	assert.True(t, t1.Sub(t0) >= 3*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestCallWithCancellableContext(t *testing.T) {
	ps.UnsubscribeAll()

	ctx, cancel := context.WithCancel(context.Background())

	ps.NewSubscriber(1, "a")

	go func() {
		time.Sleep(3 * time.Millisecond)
		cancel()
	}()

	t0 := time.Now()
	result, err := ps.Call(ctx, &ps.Msg{To: "a", Data: "Peter"}, -1)
	t1 := time.Now()

	assert.Nil(t, result)
	assert.Equal(t, context.Canceled, err)
	assert.True(t, t1.Sub(t0) >= 3*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestMsgNonRecursive(t *testing.T) {
	ps.UnsubscribeAll()

	subA := ps.NewSubscriber(10, "a")
	subAB := ps.NewSubscriber(10, "a.b")
	subABC := ps.NewSubscriber(10, "a.b.c")

	n := ps.Publish(&ps.Msg{To: "a.b.c", Data: "a data"}, &ps.MsgOpts{NonRecursive: true})
	assert.Equal(t, 1, n)

	msg := subABC.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a.b.c", msg.To)
	assert.Equal(t, "a data", msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = subAB.Get(time.Millisecond)
	assert.Nil(t, msg)

	msg = subA.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestWaitOne(t *testing.T) {
	ps.UnsubscribeAll()

	go func() {
		time.Sleep(time.Millisecond)
		n := ps.Publish(&ps.Msg{To: "a", Data: "hello"})
		assert.Equal(t, 1, n)
	}()

	msg := ps.WaitOne("a", time.Second)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "hello", msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestWaitOneImmediate(t *testing.T) {
	ps.UnsubscribeAll()

	n := ps.Publish(&ps.Msg{To: "a", Data: "hello"}, &ps.MsgOpts{Sticky: true})
	assert.Equal(t, 0, n)

	msg := ps.WaitOne("a", time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "hello", msg.Data)
	assert.Equal(t, true, msg.Old)
}

func TestWaitOneBlocking(t *testing.T) {
	ps.UnsubscribeAll()

	go func() {
		time.Sleep(3 * time.Millisecond)
		n := ps.Publish(&ps.Msg{To: "a", Data: "hello"})
		assert.Equal(t, 1, n)
	}()

	t0 := time.Now()
	msg := ps.WaitOne("a", -1)
	t1 := time.Now()

	assert.Equal(t, "a", msg.To)
	assert.Equal(t, "hello", msg.Data)
	assert.Equal(t, false, msg.Old)
	assert.True(t, t1.Sub(t0) >= 3*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestWaitOneTimeout(t *testing.T) {
	ps.UnsubscribeAll()

	t0 := time.Now()
	msg := ps.WaitOne("a", time.Millisecond*2)
	t1 := time.Now()

	assert.Nil(t, msg)
	assert.True(t, t1.Sub(t0) >= 2*time.Millisecond)
	assert.True(t, t1.Sub(t0) < 8*time.Millisecond)
}

func TestFlush(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(10, "a")

	for i := 0; i < 5; i++ {
		ps.Publish(&ps.Msg{To: "a", Data: "b"})
	}

	flushed := sub.Flush()
	assert.Equal(t, 5, flushed)
	assert.Equal(t, 0, sub.Waiting())
}

func TestCleanSticky(t *testing.T) {
	ps.UnsubscribeAll()

	ps.Publish(&ps.Msg{To: "a", Data: "a data"}, &ps.MsgOpts{Sticky: true})
	ps.Publish(&ps.Msg{To: "a.b", Data: "a.b data"}, &ps.MsgOpts{Sticky: true})
	ps.Publish(&ps.Msg{To: "a.bar", Data: "a.bar data"}, &ps.MsgOpts{Sticky: true})
	ps.Publish(&ps.Msg{To: "a.b.c", Data: "a.b.c data"}, &ps.MsgOpts{Sticky: true})

	ps.CleanSticky("a.b")

	subA := ps.NewSubscriber(10, "a")
	subAB := ps.NewSubscriber(10, "a.b")
	subAbar := ps.NewSubscriber(10, "a.bar")
	subABC := ps.NewSubscriber(10, "a.b.c")

	msgA := subA.Get(time.Second)
	require.NotNil(t, msgA)
	assert.Equal(t, "a", msgA.To)
	assert.Equal(t, "a data", msgA.Data)
	assert.Equal(t, true, msgA.Old)

	msgAB := subAB.Get(time.Millisecond)
	assert.Nil(t, msgAB)

	msgABar := subAbar.Get(time.Second)
	require.NotNil(t, msgABar)
	assert.Equal(t, "a.bar", msgABar.To)
	assert.Equal(t, "a.bar data", msgABar.Data)
	assert.Equal(t, true, msgABar.Old)

	msgABC := subABC.Get(time.Millisecond)
	assert.Nil(t, msgABC)
}

func TestFlagForDroppingOldestQueuedMessageWhenPublishingOnFullQueue(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(3, "a r")

	ps.Publish(&ps.Msg{To: "a", Data: "a1"})
	ps.Publish(&ps.Msg{To: "a", Data: "a2"})
	ps.Publish(&ps.Msg{To: "a", Data: "a3"})

	ps.Publish(&ps.Msg{To: "a", Data: "a4"})

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a2", msg.Data)

	msg = sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a3", msg.Data)

	msg = sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a4", msg.Data)

	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestGetChan(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	ch := sub.GetChan()
	ps.Publish(&ps.Msg{To: "a", Data: "b"})

	select {
	case msg := <-ch:
		assert.Equal(t, "a", msg.To)
		assert.Equal(t, "b", msg.Data)
	case <-time.After(time.Second):
		t.Error("Shouldn't timeout here")
	}
}

func TestCallWithoutSubscribersReturnsErrNotFound(t *testing.T) {
	ps.UnsubscribeAll()
	ctx := context.Background()

	_, err := ps.Call(ctx, &ps.Msg{To: "something"}, -1)
	assert.IsType(t, &ps.ErrNotFound{}, err)
}

func TestAddSubscriptionToSubscriberWithoutSubscriptions(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10)

	sub.Subscribe("a", "b")

	n := ps.Publish(&ps.Msg{To: "a", Data: 1})
	assert.Equal(t, 1, n)

	n = ps.Publish(&ps.Msg{To: "b", Data: 2})
	assert.Equal(t, 1, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, 1, msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "b", msg.To)
	assert.Equal(t, 2, msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestAddSubscriptionToSubscriberWithSubscriptions(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10, "a")

	sub.Subscribe("b")

	n := ps.Publish(&ps.Msg{To: "a", Data: 1})
	assert.Equal(t, 1, n)

	n = ps.Publish(&ps.Msg{To: "b", Data: 2})
	assert.Equal(t, 1, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, 1, msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "b", msg.To)
	assert.Equal(t, 2, msg.Data)
	assert.Equal(t, false, msg.Old)
}

func TestRepeatedSubscriptionToSubscriberOnlyReceivesMsgOnce(t *testing.T) {
	ps.UnsubscribeAll()
	sub := ps.NewSubscriber(10)

	sub.Subscribe("a")
	sub.Subscribe("a")

	n := ps.Publish(&ps.Msg{To: "a", Data: 1})
	assert.Equal(t, 1, n)

	msg := sub.Get(time.Second)
	require.NotNil(t, msg)
	assert.Equal(t, "a", msg.To)
	assert.Equal(t, 1, msg.Data)
	assert.Equal(t, false, msg.Old)

	msg = sub.Get(time.Millisecond)
	assert.Nil(t, msg)
}

func TestGetNumSubscribers(t *testing.T) {
	ps.UnsubscribeAll()

	ps.NewSubscriber(10, "a")
	assert.Equal(t, 1, ps.NumSubscribers("a"))

	ps.NewSubscriber(10, "b")
	assert.Equal(t, 1, ps.NumSubscribers("a"))
	assert.Equal(t, 1, ps.NumSubscribers("b"))

	ps.NewSubscriber(10, "b h") // Hidden subscription
	assert.Equal(t, 1, ps.NumSubscribers("b"))

	sub := ps.NewSubscriber(10, "a")
	assert.Equal(t, 2, ps.NumSubscribers("a"))

	sub.UnsubscribeAll()
	assert.Equal(t, 1, ps.NumSubscribers("a"))

	ps.UnsubscribeAll()
	assert.Equal(t, 0, ps.NumSubscribers("a"))
}

func TestHasSubscribers(t *testing.T) {
	ps.UnsubscribeAll()
	tout := time.Microsecond

	// Subscribe to events stream
	evSub := ps.NewSubscriber(10, "$events")

	// First subscriber to topic "a"
	sub0 := ps.NewSubscriber(10, "a")

	// Expect to get a HasSubsEvent with HasSubs=true
	evMsg := evSub.Get(tout)
	require.NotNil(t, evMsg)

	require.IsType(t, &ps.Event{}, evMsg.Data)
	ev := evMsg.Data.(*ps.Event)

	require.Equal(t, ps.HasSubsEventType, ev.Type)
	require.IsType(t, &ps.HasSubsEvent{}, ev.Data)

	hasSubsEv := ev.Data.(*ps.HasSubsEvent)

	require.Equal(t, "a", hasSubsEv.Topic)
	require.True(t, hasSubsEv.HasSubs)

	// Second subscriber to topic "a"
	sub1 := ps.NewSubscriber(10, "a")

	// We expect no HasSubsEvent since HasSubs is already true
	evMsg = evSub.Get(tout)
	require.Nil(t, evMsg)

	// Second subscriber ends subscription to topic "a"
	sub1.UnsubscribeAll()

	// We expect no HasSubsEvent since HasSubs is still true
	// (there is still a subscriber)
	evMsg = evSub.Get(tout)
	require.Nil(t, evMsg)

	// First subscriber ends subscription to topic "a".
	sub0.UnsubscribeAll()

	// Now there are no subscribers to topic "a" so we
	// expect to get a HasSubsEvent with HasSubs=false
	evMsg = evSub.Get(tout)
	require.NotNil(t, evMsg)

	require.IsType(t, &ps.Event{}, evMsg.Data)
	ev = evMsg.Data.(*ps.Event)

	require.Equal(t, ps.HasSubsEventType, ev.Type)
	require.IsType(t, &ps.HasSubsEvent{}, ev.Data)

	hasSubsEv = ev.Data.(*ps.HasSubsEvent)

	require.Equal(t, "a", hasSubsEv.Topic)
	require.False(t, hasSubsEv.HasSubs)
}

func TestEnqueueRotatingDeadlock(t *testing.T) {
	ps.UnsubscribeAll()

	sub := ps.NewSubscriber(1, "a r")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))
	defer cancel()

	go func() {
		ch := sub.GetChan()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			}
		}
	}()

	pch := make(chan bool, 1)
	timer := time.NewTimer(time.Second)
	for ctx.Err() == nil {
		go func() {
			ps.Publish(&ps.Msg{To: "a", Data: true})
			pch <- true
		}()

		timer.Reset(time.Second)
		select {
		case <-pch:
		case <-timer.C:
			t.FailNow()
		case <-ctx.Done():
			return
		}
		timer.Stop()
	}
}
