package benchmark

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

func (smembers *PingBenchmark) Setup() {
}

func (smembers *PingBenchmark) Cleanup() {
}

func (smembers *PingBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (smembers *PingBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult) {
	executionStartTime := time.Now()

	err := client.Ping().Err()
	if err != nil && !smembers.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "ping",
		Latency:   time.Now().Sub(executionStartTime),
	}
}

