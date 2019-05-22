package ps

import (
	"sync"
	"time"
)

// Msg is a pubsub message
type Msg struct {
	To   string
	Data string
	Old  bool
}

// MsgOpts are message options
type MsgOpts struct {
	Sticky bool
}

// Subscriber is a subscription to one or more topics
type Subscriber struct {
	ch chan *Msg
}

type subscription struct {
	sticky *Msg
	muSubs sync.RWMutex
	subs   map[*Subscriber]bool
}

var muSubs sync.RWMutex
var subs = map[string]*subscription{}

// NewSubscriber creates subscriber to topics with a queue that can hold up to size messages
func NewSubscriber(size int, topic ...string) *Subscriber {
	newSub := &Subscriber{
		ch: make(chan *Msg, size),
	}

	muSubs.Lock()
	defer muSubs.Unlock()

	for _, to := range topic {
		sub := subs[to]
		if sub == nil {
			sub = &subscription{
				sticky: nil,
				subs:   map[*Subscriber]bool{},
			}
			subs[to] = sub
		}
		sub.muSubs.Lock()
		sub.subs[newSub] = true
		sub.muSubs.Unlock()

		if sub.sticky != nil {
			newSub.ch <- sub.sticky
		}
	}

	return newSub
}

// Publish message returning with optional options. Returns number of deliveries done
func Publish(msg *Msg, opts ...*MsgOpts) int {
	delivered := 0

	var op *MsgOpts
	if len(opts) > 0 {
		op = opts[0]
	} else {
		op = &MsgOpts{}
	}

	muSubs.Lock()
	defer muSubs.Unlock()

	sub := subs[msg.To]

	if op.Sticky {
		if sub == nil {
			sub = &subscription{
				sticky: nil,
				subs:   map[*Subscriber]bool{},
			}
			subs[msg.To] = sub
		}
		msgCopy := *msg
		msgCopy.Old = true
		sub.sticky = &msgCopy
	}

	if sub != nil {
		if !op.Sticky {
			sub.sticky = nil
		}

		sub.muSubs.RLock()
		for subscriber := range sub.subs {
			subscriber.ch <- msg
			delivered++
		}
		sub.muSubs.RUnlock()
	}

	return delivered
}

// Get returns msg for a topic with a timeout (0:return inmediately, <0:block until reception, >0:block for millis or until reception)
func (s *Subscriber) Get(timeout time.Duration) *Msg {
	if timeout < 0 {
		return <-s.ch
	}

	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case msg := <-s.ch:
		return msg
	case <-t.C:
		return nil
	}
}

// Unsubscribe from topics
func (s *Subscriber) Unsubscribe(topic ...string) {
	muSubs.Lock()
	defer muSubs.Unlock()

	for _, to := range topic {
		sub := subs[to]
		if sub != nil {
			sub.muSubs.Lock()
			delete(sub.subs, s)
			if sub.canBeDeleted() {
				delete(subs, to)
			}
			sub.muSubs.Unlock()
		}
	}
}

// UnsubscribeAll unsubscribes from all topics
func (s *Subscriber) UnsubscribeAll() {
	muSubs.Lock()
	defer muSubs.Unlock()

	for to, sub := range subs {
		sub.muSubs.Lock()
		delete(sub.subs, s)
		if sub.canBeDeleted() {
			delete(subs, to)
		}
		sub.muSubs.Unlock()
	}
}

// UnsubscribeAll removes all subscribers
func UnsubscribeAll() {
	muSubs.Lock()
	defer muSubs.Unlock()

	subs = map[string]*subscription{}
}

func (s *subscription) canBeDeleted() bool {
	return len(s.subs) == 0 && s.sticky == nil
}
