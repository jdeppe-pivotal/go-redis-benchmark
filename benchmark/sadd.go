package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SaddBenchmark struct {
	config  *TestConfig
	members []string
	randInt *rand.Rand
}

var _ Runner = (*SaddBenchmark)(nil)

func NewSaddBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SaddBenchmark{
		config:  config,
		randInt: randInt,
	}
}

func (sadd *SaddBenchmark) Setup() {
	sadd.members = make([]string, sadd.config.Variant2)
	for j := 0; j < sadd.config.Variant2; j++ {
		sadd.members[j] = fmt.Sprintf("myValue-%010d", j)
	}
}

func (sadd *SaddBenchmark) Cleanup() {
}

func (sadd *SaddBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (sadd *SaddBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult) {
	key := fmt.Sprintf("mykey-%05d", sadd.randInt.Intn(sadd.config.Variant1))
	member := fmt.Sprintf("value-%010d", rand.Intn(sadd.config.Variant2))
	var err error

	executionStartTime := time.Now()
	if sadd.config.Bulk {
		err = client.SAdd(key, sadd.members).Err()
	} else {
		err = client.SAdd(key, member).Err()
	}

	if err != nil && !sadd.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "sadd",
		Latency:   time.Now().Sub(executionStartTime),
	}

	if sadd.config.Churn {
		if sadd.config.Bulk {
			err = client.SRem(key, sadd.members).Err()
		} else {
			err = client.SRem(key, member).Err()
		}
		if err != nil && !sadd.config.IgnoreErrors {
			panic(err)
		}
	}

}
