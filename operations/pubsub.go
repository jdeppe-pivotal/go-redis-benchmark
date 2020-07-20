package operations

import (
	"crypto/tls"
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

func (pubsubby *PubSubBenchmark) Setup(clients []*redis.Client) {
	pubsubby.subscribers = make([]*Subscriber, 0, pubsubby.config.Variant1)

	// Establish connections for subscribers
	for i := 0; i < pubsubby.config.Variant1; i++ {
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
		pubsubby.subscribers = append(pubsubby.subscribers, subscription)
	}

	clientOptions := redis.Options{
		Addr:     pubsubby.config.HostPort[0],
		Password: pubsubby.config.Password,
	}

	if pubsubby.config.Tls {
		clientOptions.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	pubsubby.publisher = redis.NewClient(&clientOptions)

	for _, s := range pubsubby.subscribers {
		go receiveMessages(s, pubsubby.config.Results)
	}
}

func (pubsubby *PubSubBenchmark) Cleanup() {
	for _, s := range pubsubby.subscribers {
		_ = s.pubsub.Unsubscribe(CHANNEL)
	}
}

func (pubsubby *PubSubBenchmark) ResultsPerOperation() int32 {
	return int32(pubsubby.config.Variant1)
}

func (pubsubby *PubSubBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	startTime := time.Now()
	message := fmt.Sprintf("%d", startTime.UnixNano())
	err := client.Publish(CHANNEL, message).Err()
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
