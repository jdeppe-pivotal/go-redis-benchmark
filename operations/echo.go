package operations

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type EchoBenchmark struct {
	config *TestConfig
}

var _ Runner = (*EchoBenchmark)(nil)

func NewEchoBenchmark(config *TestConfig) Runner {
	return &EchoBenchmark{
		config: config,
	}
}

func (echo *EchoBenchmark) Setup(clients []*redis.Client) {
}

func (echo *EchoBenchmark) Cleanup() {
}

func (echo *EchoBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (echo *EchoBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	executionStartTime := time.Now()

	err := client.Echo(value).Err()
	if err != nil && !echo.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "echo",
		Latency:   time.Now().Sub(executionStartTime),
	}
}
