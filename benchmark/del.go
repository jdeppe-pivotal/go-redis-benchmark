package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type DelBenchmark struct {
	config  *TestConfig
	members []string
	randInt *rand.Rand
}

var _ Runner = (*DelBenchmark)(nil)

func NewDelBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &DelBenchmark{
		config: config,
		randInt: randInt,
	}
}

func (del *DelBenchmark) Setup() {
	del.members = make([]string, del.config.Variant2)
	for j := 0; j < del.config.Variant2; j++ {
		del.members[j] = fmt.Sprintf("myValue-%d",j)
	}
}

func (del *DelBenchmark) Cleanup() {
}

func (del *DelBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (del *DelBenchmark) DoOneOperation(client *redis.Client, results chan time.Duration) {
	key := fmt.Sprintf("mykey-%d", del.randInt.Intn(del.config.Variant1))

	err := client.SAdd(key, del.members).Err()
	if err != nil && !del.config.IgnoreErrors {
		panic(err)
	}

	executionStartTime := time.Now()
	err = client.Del(key).Err()
	if err != nil && !del.config.IgnoreErrors {
		panic(err)
	}

	latency := time.Now().Sub(executionStartTime)

	results <- latency
}

