package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SaddBenchmark struct {
	config *TestConfig
}

func NewSaddBenchmark(config *TestConfig) Runner {
	return &SaddBenchmark{config}
}

var _ Runner = (*SaddBenchmark)(nil)

func (sadd *SaddBenchmark) Launch() {
	clients := make([]*redis.Client, 0, sadd.config.ClientCount)

	for i := 0; i < sadd.config.ClientCount; i++ {
		client := redis.NewClient(&redis.Options{
			Addr: sadd.config.HostPort[i % len(sadd.config.HostPort)],
		})

		clients = append(clients, client)
	}

	testStartTime := time.Now()

	results := make(chan time.Duration)

	for _, c := range clients {
		go sadd.doSadd(c, results)
	}

	latencies := make(map[int]int)
	resultCount := new(int)
	*resultCount = 0

	tickQuitter := make(chan struct{})
	go throughputTicker(resultCount, tickQuitter)

	// Process results
	for r := range results {
		latencies[int(r.Milliseconds()) + 1]++

		*resultCount++
		if *resultCount == sadd.config.Iterations {
			break
		}
	}

	// Stop the ticker
	tickQuitter <- struct{}{}

	fmt.Println()
	printSummary(latencies)

	throughput := float64(sadd.config.Iterations) / time.Now().Sub(testStartTime).Seconds()

	fmt.Println()
	fmt.Printf("Clients:    %d\n", sadd.config.ClientCount)
	fmt.Printf("Operations: %d\n", *resultCount)
	fmt.Printf("Throughput: %0.2f ops/sec\n", throughput)
	fmt.Println()
}

func (sadd *SaddBenchmark) doSadd(client *redis.Client, results chan time.Duration) {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < sadd.config.ClientIterations; i++ {
		executionStartTime := time.Now()

		key := fmt.Sprintf("mykey-%d", randInt.Intn(sadd.config.Variant1))
		member := fmt.Sprintf("value-%d", rand.Intn(sadd.config.Variant2))
		err := client.SAdd(key, member).Err()
		if err != nil && !sadd.config.IgnoreErrors {
			panic(err)
		}

		if sadd.config.Churn {
			err := client.SRem(key, member).Err()
			if err != nil && !sadd.config.IgnoreErrors {
				panic(err)
			}
		}

		latency := time.Now().Sub(executionStartTime)

		results <- latency
	}
}
