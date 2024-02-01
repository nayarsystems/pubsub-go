package ps

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Msg is a pubsub message
type Msg struct {
	To   string
	Data interface{}
	Old  bool
	Res  string
	Err  error
}

// MsgOpts are message options
type MsgOpts struct {
	Sticky       bool
	NonRecursive bool
}

// Subscriber is a subscription to one or more topics
type Subscriber struct {
	myTopics map[string]bool
	ch       chan *Msg
	overflow uint32
}

type EventType uint

const (
	HasSubsEventType EventType = iota
)

type Event struct {
	Type EventType
	Data interface{}
}

type HasSubsEvent struct {
	Topic   string
	HasSubs bool
}

type subscriberInfo struct {
	hidden             bool
	ignoreSticky       bool
	stickyFromChildren bool
	rotateWhenFull     bool
}

type topicInfo struct {
	sticky *Msg
	subs   map[*Subscriber]*subscriberInfo
}

// ErrNotFound returned when doing a call on a topic with no subscribers
type ErrNotFound struct {
}

func (e *ErrNotFound) Error() string { return "No subscriber for that topic" }

var muTopics sync.RWMutex
var topics = map[string]*topicInfo{}

// NewSubscriber creates subscriber to topics with a queue that can hold up to size messages
// Topics are separated by "." (e.g.: "a.b.c") and can optionally have an " " and some flags (e.g. "a.b.c hs").
//
// Possible flags:
//
//	h: this subscriber is hidden so it doesn't count as delivered on Publish.
//	s: when subscribing don't receive sticky for this topic.
//	S: receive sticky from this topic and its children.
//	r: when publishing on a full queue remove oldest element and insert
func NewSubscriber(size int, topic ...string) *Subscriber {
	newSub := &Subscriber{
		myTopics: map[string]bool{},
		ch:       make(chan *Msg, size),
	}

	newSub.registerTopics(topic...)

	return newSub
}

func (s *Subscriber) registerTopics(topic ...string) {
	muTopics.Lock()
	defer muTopics.Unlock()

	for _, fullTo := range topic {
		to := strings.Split(fullTo, " ")[0]
		s.addTopic(to)

		toInfo := topics[to]
		if toInfo == nil {
			toInfo = &topicInfo{
				sticky: nil,
				subs:   map[*Subscriber]*subscriberInfo{},
			}
			topics[to] = toInfo
			publishEvent(&Event{Type: HasSubsEventType, Data: &HasSubsEvent{Topic: to, HasSubs: true}})
		}
		subInfo := parseFlags(fullTo)
		toInfo.subs[s] = subInfo

		if toInfo.sticky != nil && !subInfo.ignoreSticky {
			s.enqueue(toInfo.sticky)
		}

		if subInfo.stickyFromChildren {
			for otherTo, otherToInfo := range topics {
				isChild := strings.HasPrefix(otherTo, to+".")
				if otherToInfo.sticky != nil && isChild {
					s.enqueue(otherToInfo.sticky)
				}
			}
		}
	}
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
			case 'r':
				info.rotateWhenFull = true
			}
		}
	}

	return info
}

// Publish message with options. Returns number of deliveries done
func Publish(msg *Msg, opts ...*MsgOpts) int {
	var msgOpts *MsgOpts
	if len(opts) > 0 {
		msgOpts = opts[0]
	} else {
		msgOpts = &MsgOpts{}
	}

	muTopics.Lock()
	defer muTopics.Unlock()

	return publish(msg, msgOpts)
}

func publish(msg *Msg, msgOpts *MsgOpts) int {
	if msgOpts.Sticky {
		storeSticky(msg)
	}

	delivered := 0
	toParts := strings.Split(msg.To, ".")

	for len(toParts) > 0 {
		to := strings.Join(toParts, ".")
		toParts = toParts[:len(toParts)-1]

		delivered += publishMsg(to, msg, msgOpts)

		if msgOpts.NonRecursive {
			break
		}
	}

	return delivered
}

func storeSticky(msg *Msg) {
	toInfo := topics[msg.To]
	if toInfo == nil {
		toInfo = &topicInfo{
			sticky: nil,
			subs:   map[*Subscriber]*subscriberInfo{},
		}
		topics[msg.To] = toInfo
	}
	msgCopy := *msg
	msgCopy.Old = true
	toInfo.sticky = &msgCopy
}

func publishMsg(to string, msg *Msg, msgOpts *MsgOpts) int {
	delivered := 0
	toInfo := topics[to]
	if toInfo != nil {
		if !msgOpts.Sticky && to == msg.To {
			toInfo.sticky = nil
		}

		for subscriber, subInfo := range toInfo.subs {
			if subInfo.rotateWhenFull {
				subscriber.enqueueRotating(msg)
			} else {
				queued := subscriber.enqueue(msg)
				if !queued {
					continue
				}
			}

			if !subInfo.hidden {
				delivered++
			}
		}
	}
	return delivered
}

func (s *Subscriber) addTopic(to string) {
	s.myTopics[to] = true
}

func (s *Subscriber) removeTopic(to string) {
	delete(s.myTopics, to)
}

func (s *Subscriber) getTopics() []string {
	toSubs := []string{}
	for to := range s.myTopics {
		toSubs = append(toSubs, to)
	}

	return toSubs
}

func (s *Subscriber) enqueueRotating(msg *Msg) {
	inserted := false
	for !inserted {
		select {
		case s.ch <- msg:
			inserted = true
		default:
			select {
			case <-s.ch:
			default:
			}
		}
	}
}

func (s *Subscriber) enqueue(msg *Msg) bool {
	select {
	case s.ch <- msg:
		return true
	default:
		atomic.AddUint32(&s.overflow, 1)
		return false
	}
}

// GetWithCtx returns msg for a topic with a timeout (0:return inmediately, <0:block until reception, >0:block for timeout or until reception)
func (s *Subscriber) GetWithCtx(ctx context.Context, timeout time.Duration) *Msg {
	if timeout >= 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// Let's check the message channel first.
	// In case there is a message in the channel it will
	// ensure the message is read before the timeout is reached.
	// e.g: len(s.ch) > 0 && timeout=0
	select {
	case msg := <-s.ch:
		return msg
	default:
	}
	select {
	case msg := <-s.ch:
		return msg
	case <-ctx.Done():
		return nil
	}
}

// Get returns msg for a topic with a timeout (0:return inmediately, <0:block until reception, >0:block for timeout or until reception)
func (s *Subscriber) Get(timeout time.Duration) *Msg {
	return s.GetWithCtx(context.Background(), timeout)
}

// GetChan returns channel with messages for this subscriber
func (s *Subscriber) GetChan() <-chan *Msg {
	return s.ch
}

// Subscribe subscribes to topics (see NewSubscriber)
func (s *Subscriber) Subscribe(topic ...string) {
	s.registerTopics(topic...)
}

// Unsubscribe from topics
func (s *Subscriber) Unsubscribe(topic ...string) {
	muTopics.Lock()
	defer muTopics.Unlock()

	for _, to := range topic {
		s.removeTopic(to)

		toInfo := topics[to]
		if toInfo != nil {
			if _, ok := toInfo.subs[s]; !ok {
				panic("unregisterd subscriber")
			}
			delete(toInfo.subs, s)
			if toInfo.canBeDeleted() {
				delete(topics, to)
				publishEvent(&Event{Type: HasSubsEventType, Data: &HasSubsEvent{Topic: to, HasSubs: false}})
			}
		}
	}
}

func publishEvent(event *Event) {
	publish(&Msg{To: "$events", Data: event}, &MsgOpts{})
}

// UnsubscribeAll unsubscribes from all topics
func (s *Subscriber) UnsubscribeAll() {
	s.Unsubscribe(s.getTopics()...)
}

// UnsubscribeAll removes all subscribers
func UnsubscribeAll() {
	muTopics.Lock()
	defer muTopics.Unlock()

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

// Flush removes waiting messages returning how many were queued
func (s *Subscriber) Flush() int {
	removed := 0
	for len(s.ch) > 0 {
		select {
		case <-s.ch:
			removed++
		default:
			return removed
		}
	}
	return removed
}

// Call publishes message waiting for receiving response (using msg.Answer) with timeout (<0:block until reception, >0:block for timeout or until reception)
func Call(ctx context.Context, msg *Msg, timeout time.Duration, opts ...*MsgOpts) (interface{}, error) {
	res, err := newResponsePath()
	if err != nil {
		return nil, err
	}

	sub := NewSubscriber(1, res)
	defer sub.UnsubscribeAll()

	delivered := Publish(&Msg{To: msg.To, Data: msg.Data, Res: res}, opts...)
	if delivered == 0 {
		return nil, &ErrNotFound{}
	}

	if timeout >= 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	select {
	case msg := <-sub.GetChan():
		return msg.Data, msg.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newResponsePath() (string, error) {
	numBytes := 8
	data := make([]byte, numBytes)
	n, err := rand.Read(data)
	if err != nil {
		return "", err
	}

	if n != numBytes {
		return "", fmt.Errorf("expected %d bytes got %d", numBytes, n)
	}

	return "$ret." + hex.EncodeToString(data), nil
}

// WaitOneWithCtx waits for message on topic "topic" and returns first one with a timeout (0:return inmediately, <0:block until reception, >0:block for timeout or until reception)
func WaitOneWithCtx(ctx context.Context, topic string, timeout time.Duration) *Msg {
	sub := NewSubscriber(1, topic)
	defer sub.UnsubscribeAll()

	return sub.GetWithCtx(ctx, timeout)
}

// WaitOne waits for message on topic "topic" and returns first one with a timeout (0:return inmediately, <0:block until reception, >0:block for timeout or until reception)
func WaitOne(topic string, timeout time.Duration) *Msg {
	return WaitOneWithCtx(context.Background(), topic, timeout)
}

// Answer replies to this Msg publishing another Msg to msg.Res
func (m *Msg) Answer(data interface{}, err error) {
	if m.Res != "" {
		Publish(&Msg{To: m.Res, Data: data, Err: err})
	}
}

// CleanSticky removes sticky from topic and its children
func CleanSticky(to string) {
	muTopics.Lock()
	defer muTopics.Unlock()

	for t, toInfo := range topics {
		if t == to || strings.HasPrefix(t, to+".") {
			toInfo.sticky = nil
		}
	}
}

// NumSubscribers returns number of subscribers to topic
func NumSubscribers(to string) int {
	muTopics.Lock()
	defer muTopics.Unlock()

	hidden := 0
	toInfo := topics[to]
	if toInfo == nil {
		return 0
	} else {
		for _, suInfo := range toInfo.subs {
			if suInfo.hidden {
				hidden++
			}
		}
	}

	return len(toInfo.subs) - hidden
}
