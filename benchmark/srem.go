package benchmark

import (
	"fmt"
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
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SremBenchmark{
		config: config,
		randInt: randInt,
	}
}

func (srem *SremBenchmark) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: srem.config.HostPort[0],
	})
	srem.members = make([]string, srem.config.Variant2)
	for j := 0; j < srem.config.Variant2; j++ {
		srem.members[j] = fmt.Sprintf("myValue-%010d",j)
	}

	for i := 0; i < srem.config.Variant1; i++ {
		client.SAdd(fmt.Sprintf("mykey-%05d",i), srem.members)
	}

	client.Close()
}

func (srem *SremBenchmark) Cleanup() {}

func (srem *SremBenchmark) ResultsPerOperation() int32 {
	return 2
}

func (srem *SremBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult) {
	key := fmt.Sprintf("mykey-%05d", srem.randInt.Intn(srem.config.Variant1))
	value := fmt.Sprintf("myValue-%010d", srem.randInt.Intn(srem.config.Variant2))
	var err error

	executionStartTime := time.Now()
	if srem.config.Bulk {
		err = client.SRem(key, srem.members).Err()
	} else {
		err = client.SRem(key, value).Err()
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
		err = client.SAdd(key, value).Err()
	}
	if err != nil && !srem.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "sadd",
		Latency:   time.Now().Sub(saddStart),
	}
}

