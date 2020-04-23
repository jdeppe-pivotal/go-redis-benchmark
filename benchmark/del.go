package benchmark

import (
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
		del.members[j] = CreateValue(j)
	}
}

func (del *DelBenchmark) Cleanup() {
}

func (del *DelBenchmark) ResultsPerOperation() int32 {
	return 2
}

func (del *DelBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult) {
	key := CreateKey(del.randInt.Intn(del.config.Variant1))

	saddStart := time.Now()
	err := client.SAdd(key, del.members).Err()
	if err != nil && !del.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "sadd",
		Latency:   time.Now().Sub(saddStart),
	}

	executionStartTime := time.Now()
	err = client.Del(key).Err()
	if err != nil && !del.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "del",
		Latency:   time.Now().Sub(executionStartTime),
	}
}

