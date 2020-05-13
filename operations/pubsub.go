package operations

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"strconv"
	"time"
)

type PubSubBenchmark struct {
	config      *TestConfig
	subscribers []*Subscriber
	publisher   *redis.Client
}

type Subscriber struct {
	id     int
	pubsub *redis.PubSub
}

const (
	CHANNEL = "benchmarkChannel"
)

var _ Runner = (*PubSubBenchmark)(nil)

func NewPubSubBenchmark(config *TestConfig) *PubSubBenchmark {
	return &PubSubBenchmark{
		config: config,
	}
}

func (bm *PubSubBenchmark) Setup(clients []*redis.Client) {
	bm.subscribers = make([]*Subscriber, 0, bm.config.Variant1)

	// Establish connections for subscribers
	for i := 0; i < bm.config.Variant1; i++ {
		client := clients[i%len(clients)]
		pubsub := client.Subscribe(CHANNEL)

		err := waitForSubscription(pubsub)
		if err != nil {
			panic(err)
		}

		subscription := &Subscriber{
			id:     i,
			pubsub: pubsub,
		}
		bm.subscribers = append(bm.subscribers, subscription)
	}

	bm.publisher = redis.NewClient(&redis.Options{
		Addr:     bm.config.HostPort[0],
		Password: bm.config.Password,
	})

	for _, s := range bm.subscribers {
		go receiveMessages(s, bm.config.Results)
	}
}

func (bm *PubSubBenchmark) Cleanup() {
	for _, s := range bm.subscribers {
		_ = s.pubsub.Unsubscribe(CHANNEL)
	}
}

func (bm *PubSubBenchmark) ResultsPerOperation() int32 {
	return int32(bm.config.Variant1)
}

func (bm *PubSubBenchmark) DoOneOperation(publisher *redis.Client, results chan *OperationResult, key string, value string) {
	startTime := time.Now()
	message := fmt.Sprintf("%d", startTime.UnixNano())
	err := publisher.Publish(CHANNEL, message).Err()
	if err != nil {
		panic(err)
	}
}

func receiveMessages(subscriber *Subscriber, results chan *OperationResult) {
	for msg := range subscriber.pubsub.Channel() {
		messageReceived := time.Now().UnixNano()

		messageCreated, err := strconv.Atoi(msg.Payload)
		if err != nil {
			panic(err)
		}

		latency := messageReceived - int64(messageCreated)
		results <- &OperationResult{
			Operation: "pubsub",
			Latency:   time.Duration(latency),
		}

		//fmt.Printf("Received: %s\n", msg.Payload)
	}
}

func waitForSubscription(pubSub *redis.PubSub) error {
	msgi, err := pubSub.ReceiveTimeout(time.Second * 2)
	if err != nil {
		return err
	}

	switch msg := msgi.(type) {
	case *redis.Subscription:
		return nil
	default:
		panic(fmt.Sprintf("Unexpected message waiting for subscription: %v", msg))
	}
}
