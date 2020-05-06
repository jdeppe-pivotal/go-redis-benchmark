package operations

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type PingBenchmark struct {
	config  *TestConfig
}

var _ Runner = (*PingBenchmark)(nil)

func NewPingBenchmark(config *TestConfig) Runner {
	return &PingBenchmark{
		config: config,
	}
}

func (ping *PingBenchmark) Setup() {
}

func (ping *PingBenchmark) Cleanup() {
}

func (ping *PingBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (ping *PingBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, value string) {
	executionStartTime := time.Now()

	err := client.Ping().Err()
	if err != nil && !ping.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "ping",
		Latency:   time.Now().Sub(executionStartTime),
	}
}

