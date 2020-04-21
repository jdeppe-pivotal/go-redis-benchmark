package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SaddBenchmark struct {
	config  *TestConfig
	randInt *rand.Rand
}

var _ Runner = (*SaddBenchmark)(nil)

func NewSaddBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SaddBenchmark{
		config: config,
		randInt: randInt,
	}
}

func (sadd *SaddBenchmark) Setup() {
}

func (sadd *SaddBenchmark) Cleanup() {
}

func (sadd *SaddBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (sadd *SaddBenchmark) DoOneOperation(client *redis.Client, results chan time.Duration) {
	executionStartTime := time.Now()

	key := fmt.Sprintf("mykey-%d", sadd.randInt.Intn(sadd.config.Variant1))
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
