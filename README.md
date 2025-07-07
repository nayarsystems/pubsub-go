# pubsub-go

A high-performance, thread-safe publish-subscribe message broker library for Go with hierarchical topics and advanced subscriber patterns.

## Why Pub/Sub?

The publish-subscribe pattern offers several key advantages for building scalable applications:

- **Decoupling**: Publishers and subscribers don't need to know about each other
- **Scalability**: Multiple subscribers can process messages independently  
- **Flexibility**: Dynamic subscription and unsubscription at runtime
- **Asynchronous Processing**: Non-blocking message delivery
- **Event-Driven Architecture**: Natural fit for reactive systems

## Features

- 🏗️ **Hierarchical Topics**: Dot-notation topics with inheritance (`a.b.c` → `a.b` → `a`)
- 📌 **Sticky Messages**: Persistent messages delivered to new subscribers
- 🔄 **Request-Response**: RPC-style communication with timeouts
- 🎯 **Subscriber Flags**: Fine-grained control over message delivery
- 📊 **Event System**: Monitor subscription changes
- 🔒 **Thread-Safe**: Concurrent access with RWMutex protection
- ⚡ **High Performance**: Channel-based message queuing

## Installation

```bash
go get github.com/nayarsystems/pubsub-go/ps
```

## Quick Start

### Basic Pub/Sub

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/nayarsystems/pubsub-go/ps"
)

func main() {
    // Create a new pub/sub instance
    pubsub := ps.New()
    
    // Create a subscriber
    sub := pubsub.Sub("user.login")
    
    // Publish a message
    pubsub.Pub("Hello, World!", "user.login")
    
    // Receive the message
    msg := sub.Get(time.Second)
    if msg != nil {
        fmt.Printf("Received: %v\n", msg.Data)
    }
}
```

### Hierarchical Topics

```go
pubsub := ps.New()

// Subscribe to parent topic
sub := pubsub.Sub("events")

// Messages to child topics are delivered to parent subscribers
pubsub.Pub("User logged in", "events.user.login")
pubsub.Pub("Order created", "events.order.create")

// Both messages will be received by the "events" subscriber
for i := 0; i < 2; i++ {
    msg := sub.Get(time.Second)
    fmt.Printf("Event: %v on topic: %s\n", msg.Data, msg.To)
}
```

### Sticky Messages

```go
pubsub := ps.New()

// Publish a sticky message (persists until cleared)
pubsub.PubSticky("System ready", "status")

// New subscribers immediately receive sticky messages
sub := pubsub.Sub("status")
msg := sub.Get(time.Second)
fmt.Printf("Status: %v (sticky: %v)\n", msg.Data, msg.Old)
```

### Request-Response Pattern

```go
pubsub := ps.New()

// Subscriber that responds to requests
go func() {
    sub := pubsub.Sub("math.add")
    for {
        msg := sub.Get(time.Hour)
        if msg == nil {
            break
        }
        
        // Process request and send response
        nums := msg.Data.([]int)
        result := nums[0] + nums[1]
        pubsub.Pub(result, msg.Res)
    }
}()

// Make a request and wait for response
result, err := pubsub.Call([]int{5, 3}, "math.add", time.Second)
if err == nil {
    fmt.Printf("5 + 3 = %v\n", result)
}
```

### Advanced Subscriber Flags

```go
pubsub := ps.New()

// Subscribe with flags:
// 'S' - receive sticky messages from topic and children
// 'r' - rotate queue when full (drop oldest messages)
sub := pubsub.SubFlag("events", "Sr")

// Subscribe as hidden (doesn't count in delivery statistics)
hiddenSub := pubsub.SubFlag("events", "h")

// Skip sticky messages for this topic
noStickySub := pubsub.SubFlag("events", "s")
```

### Context-Aware Operations

```go
import "context"

pubsub := ps.New()
sub := pubsub.Sub("data")

// Use context for cancellation
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

msg := sub.GetWithCtx(ctx)
if msg != nil {
    fmt.Printf("Received: %v\n", msg.Data)
}
```

### Event Monitoring

```go
pubsub := ps.New()

// Monitor subscription events
eventSub := pubsub.Sub("$events")

go func() {
    for {
        msg := eventSub.Get(time.Hour)
        if msg == nil {
            break
        }
        
        event := msg.Data.(ps.HasSubsEvent)
        fmt.Printf("Topic %s now has %d subscribers\n", 
            event.Topic, event.Count)
    }
}()

// Create subscriptions (will trigger events)
sub1 := pubsub.Sub("notifications")
sub2 := pubsub.Sub("notifications")
```

## API Reference

### PubSub Methods

- `Pub(data interface{}, topics ...string)` - Publish message to topics
- `PubSticky(data interface{}, topic string)` - Publish sticky message
- `Sub(topics ...string) *Subscriber` - Create subscriber
- `SubFlag(topic, flags string) *Subscriber` - Create subscriber with flags
- `Call(data interface{}, topic string, timeout time.Duration)` - Request-response
- `ClearSticky(topic string)` - Clear sticky message
- `NumSubscribers(topic string) int` - Get subscriber count

### Subscriber Methods

- `Get(timeout time.Duration) *Msg` - Get message with timeout
- `GetWithCtx(ctx context.Context) *Msg` - Get message with context
- `GetChan() <-chan *Msg` - Get message channel
- `WaitOne(timeout time.Duration) *Msg` - Wait for one message
- `Flush()` - Clear pending messages
- `Overflow() int` - Get dropped message count
- `Waiting() int` - Get pending message count

### Subscriber Flags

- `h` - Hidden subscriber (doesn't count in delivery stats)
- `s` - Skip sticky messages for this topic
- `S` - Receive sticky messages from topic and children  
- `r` - Rotate queue when full (drop oldest messages)

## Performance

The library is optimized for high-throughput scenarios:

- Channel-based message queuing for minimal latency
- RWMutex for efficient concurrent read access
- Buffered channels prevent blocking publishers
- Optional message rotation to handle slow consumers

Run benchmarks:

```bash
cd ps && go test -bench=.
```

## Testing

```bash
cd ps && go test -v          # Run all tests
cd ps && go test -race       # Test with race detection
cd ps && go test -cover      # Test with coverage
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please ensure:

- Tests pass: `go test -v`
- No race conditions: `go test -race`
- Code follows Go conventions
- New features include tests and documentation