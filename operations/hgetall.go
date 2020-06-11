package operations

import (
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type HgetallBenchmark struct {
	config  *TestConfig
	randInt *rand.Rand
}

var _ Runner = (*HgetallBenchmark)(nil)

func NewHgetallBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &HgetallBenchmark{
		config:  config,
		randInt: randInt,
	}
}

func (hgetall *HgetallBenchmark) Setup(clients []*redis.Client) {
	client := clients[0]

	for i := 0; i < hgetall.config.Variant1; i++ {
		key := CreateKey(i)
		client.Del(key)
		for j := 0; j < hgetall.config.Variant2; j++ {
			field := CreateField(j)
			value := CreateValue(j)
			err := client.HSet(key, field, value).Err()
			if err != nil && !hgetall.config.IgnoreErrors {
				panic(err)
			}
		}
	}
}

func (hgetall *HgetallBenchmark) Cleanup() {
}

func (hgetall *HgetallBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (hgetall *HgetallBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	executionStartTime := time.Now()

	err := client.HGetAll(key).Err()
	if err != nil && !hgetall.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "hgetall",
		Latency:   time.Now().Sub(executionStartTime),
	}
}
