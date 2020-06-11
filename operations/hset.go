package operations

import (
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type HsetBenchmark struct {
	config  *TestConfig
	randInt *rand.Rand
}

var _ Runner = (*HsetBenchmark)(nil)

func NewHsetBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &HsetBenchmark{
		config:  config,
		randInt: randInt,
	}
}

func (hset *HsetBenchmark) Setup(clients []*redis.Client) {
}

func (hset *HsetBenchmark) Cleanup() {
}

func (hset *HsetBenchmark) ResultsPerOperation() int32 {
	if hset.config.Churn {
		return 2
	} else {
		return 1
	}
}

func (hset *HsetBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	var err error

	executionStartTime := time.Now()
	err = client.HSet(key, field, value).Err()

	if err != nil && !hset.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "hset",
		Latency:   time.Now().Sub(executionStartTime),
	}

	if hset.config.Churn {
		hdelStartTime := time.Now()
		err = client.HDel(key, field).Err()
		if err != nil && !hset.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "hdel",
			Latency:   time.Now().Sub(hdelStartTime),
		}
	}

}
