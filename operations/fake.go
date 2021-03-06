package operations

import "github.com/go-redis/redis/v7"

type FakeBenchmark struct {
	config *TestConfig
}

var _ Runner = (*FakeBenchmark)(nil)

func NewFakeBenchmark(config *TestConfig) Runner {
	return &FakeBenchmark{
		config: config,
	}
}

func (ping *FakeBenchmark) Setup(clients []*redis.Client) {
}

func (ping *FakeBenchmark) Cleanup() {
}

func (ping *FakeBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (ping *FakeBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
}
