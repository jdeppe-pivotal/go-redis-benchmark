package operations

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type SetBenchmark struct {
	config *TestConfig
}

var _ Runner = (*SetBenchmark)(nil)

func NewSetBenchmark(config *TestConfig) Runner {
	return &SetBenchmark{
		config: config,
	}
}

func (set *SetBenchmark) Setup(clients []*redis.Client) {
}

func (set *SetBenchmark) Cleanup() {
}

func (set *SetBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (set *SetBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	executionStartTime := time.Now()

	err := client.Set(key, value, 0).Err()
	if err != nil && !set.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "set",
		Latency:   time.Now().Sub(executionStartTime),
	}
}
