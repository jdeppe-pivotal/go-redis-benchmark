package operations

import (
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SremBenchmark struct {
	config  *TestConfig
	members []string
	randInt *rand.Rand
}

var _ Runner = (*SremBenchmark)(nil)

func NewSremBenchmark(config *TestConfig) Runner {
	return &SremBenchmark{
		config:  config,
		randInt: randInt,
	}
}

func (srem *SremBenchmark) Setup(clients []*redis.Client) {
	client := clients[0]

	srem.members = make([]string, srem.config.Variant2)
	for j := 0; j < srem.config.Variant2; j++ {
		srem.members[j] = CreateValue(j)
	}

	for i := 0; i < srem.config.Variant1; i++ {
		client.SAdd(CreateKey(i), srem.members)
	}
}

func (srem *SremBenchmark) Cleanup() {}

func (srem *SremBenchmark) ResultsPerOperation() int32 {
	return 2
}

func (srem *SremBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, field string, value string) {
	var err error

	executionStartTime := time.Now()
	if srem.config.Bulk {
		err = client.SRem(key, srem.members).Err()
	} else {
		err = client.SRem(key, field).Err()
	}
	if err != nil && !srem.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "srem",
		Latency:   time.Now().Sub(executionStartTime),
	}

	saddStart := time.Now()
	if srem.config.Bulk {
		err = client.SAdd(key, srem.members).Err()
	} else {
		err = client.SAdd(key, field).Err()
	}
	if err != nil && !srem.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "sadd",
		Latency:   time.Now().Sub(saddStart),
	}
}
