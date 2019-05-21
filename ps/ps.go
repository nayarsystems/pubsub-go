package ps

import "sync"

// Msg is a pubsub message
type Msg struct {
	To   string
	Data string
}

// Subscriber is a subscription to one or more topics
type Subscriber struct {
	ch chan *Msg
}

type subscription struct {
	sticky interface{}
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
				sticky: false,
				subs:   map[*Subscriber]bool{},
			}
			subs[to] = sub
		}
		sub.muSubs.Lock()
		sub.subs[newSub] = true
		sub.muSubs.Unlock()
	}

	return newSub
}

// Publish message returning number of deliveries done
func Publish(msg *Msg) int {
	delivered := 0

	muSubs.RLock()
	subscription := subs[msg.To]
	muSubs.RUnlock()

	if subscription != nil {
		subscription.muSubs.RLock()
		for sub := range subscription.subs {
			sub.ch <- msg
			delivered++
		}
		subscription.muSubs.RUnlock()
	}

	return delivered
}

// Get returns msg for a topic with a timeout (0:return inmediately, <0:block until reception, >0:block for millis or until reception)
func (s *Subscriber) Get(timeout int) *Msg {
	return <-s.ch
}
