package ps

import (
	"strings"
	"sync"
	"sync/atomic"
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
	ch       chan *Msg
	overflow uint32
}

type subscriberInfo struct {
	hidden             bool
	ignoreSticky       bool
	stickyFromChildren bool
}

type topicInfo struct {
	sticky *Msg
	muSubs sync.RWMutex
	subs   map[*Subscriber]*subscriberInfo
}

var muSubs sync.RWMutex
var topics = map[string]*topicInfo{}

// NewSubscriber creates subscriber to topics with a queue that can hold up to size messages
func NewSubscriber(size int, topic ...string) *Subscriber {
	newSub := &Subscriber{
		ch: make(chan *Msg, size),
	}

	muSubs.Lock()
	defer muSubs.Unlock()

	for _, fullTo := range topic {
		to := strings.Split(fullTo, " ")[0]
		toInfo := topics[to]
		if toInfo == nil {
			toInfo = &topicInfo{
				sticky: nil,
				subs:   map[*Subscriber]*subscriberInfo{},
			}
			topics[to] = toInfo
		}
		toInfo.muSubs.Lock()
		subInfo := parseFlags(fullTo)
		toInfo.subs[newSub] = subInfo
		toInfo.muSubs.Unlock()

		for otherTo, otherToInfo := range topics {
			isChild := strings.HasPrefix(otherTo, to+".")
			if otherToInfo.sticky != nil && !subInfo.ignoreSticky && (otherTo == to || (isChild && subInfo.stickyFromChildren)) {
				newSub.ch <- otherToInfo.sticky
			}
		}
	}

	return newSub
}

func parseFlags(to string) *subscriberInfo {
	info := &subscriberInfo{}
	parts := strings.Split(to, " ")

	if len(parts) == 2 {
		for _, c := range parts[1] {
			switch c {
			case 'h':
				info.hidden = true
			case 's':
				info.ignoreSticky = true
			case 'S':
				info.stickyFromChildren = true
			}
		}
	}

	return info
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

	toParts := strings.Split(msg.To, ".")

	for len(toParts) > 0 {
		to := strings.Join(toParts, ".")
		toParts = toParts[:len(toParts)-1]
		toInfo := topics[to]

		if op.Sticky && to == msg.To {
			if toInfo == nil {
				toInfo = &topicInfo{
					sticky: nil,
					subs:   map[*Subscriber]*subscriberInfo{},
				}
				topics[to] = toInfo
			}
			msgCopy := *msg
			msgCopy.Old = true
			toInfo.sticky = &msgCopy
		}

		if toInfo != nil {
			if !op.Sticky {
				toInfo.sticky = nil
			}

			toInfo.muSubs.RLock()
			for subscriber, subInfo := range toInfo.subs {
				select {
				case subscriber.ch <- msg:
				default:
					atomic.AddUint32(&subscriber.overflow, 1)
					continue
				}
				if !subInfo.hidden {
					delivered++
				}
			}
			toInfo.muSubs.RUnlock()
		}
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
		toInfo := topics[to]
		if toInfo != nil {
			toInfo.muSubs.Lock()
			delete(toInfo.subs, s)
			if toInfo.canBeDeleted() {
				delete(topics, to)
			}
			toInfo.muSubs.Unlock()
		}
	}
}

// UnsubscribeAll unsubscribes from all topics
func (s *Subscriber) UnsubscribeAll() {
	muSubs.Lock()
	defer muSubs.Unlock()

	for to, toInfo := range topics {
		toInfo.muSubs.Lock()
		delete(toInfo.subs, s)
		if toInfo.canBeDeleted() {
			delete(topics, to)
		}
		toInfo.muSubs.Unlock()
	}
}

// UnsubscribeAll removes all subscribers
func UnsubscribeAll() {
	muSubs.Lock()
	defer muSubs.Unlock()

	topics = map[string]*topicInfo{}
}

func (s *topicInfo) canBeDeleted() bool {
	return len(s.subs) == 0 && s.sticky == nil
}

// Overflow returns number of messages that overflowed when published
func (s *Subscriber) Overflow() uint32 {
	return atomic.LoadUint32(&s.overflow)
}

// Waiting returns number of messages waiting to be read
func (s *Subscriber) Waiting() int {
	return len(s.ch)
}
