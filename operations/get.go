package operations

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type GetBenchmark struct {
	config *TestConfig
}

var _ Runner = (*GetBenchmark)(nil)

func NewGetBenchmark(config *TestConfig) Runner {
	return &GetBenchmark{
		config: config,
	}
}

func (get *GetBenchmark) Setup(clients []*redis.Client) {
	if get.config.Load {
		client := clients[0]
		for i := 0; i < get.config.Variant1; i++ {
			key := CreateKey(i)
			err := client.Set(key, key, 0).Err()
			if err != nil {
				panic(err)
			}
		}
	}
}

func (get *GetBenchmark) Cleanup() {
}

func (get *GetBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (get *GetBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	executionStartTime := time.Now()

	err := client.Get(key).Err()
	if err != nil && !get.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "get",
		Latency:   time.Now().Sub(executionStartTime),
	}
}
