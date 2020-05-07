package operations

import (
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SmembersBenchmark struct {
	config  *TestConfig
	randInt *rand.Rand
}

var _ Runner = (*SmembersBenchmark)(nil)

func NewSmembersBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SmembersBenchmark{
		config: config,
		randInt: randInt,
	}
}

func (smembers *SmembersBenchmark) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: smembers.config.HostPort[0],
		Password: smembers.config.Password,
	})

	for i := 0; i < smembers.config.Variant1; i++ {
		key := CreateKey(i)
		client.Del(key)
		for j := 0; j < smembers.config.Variant2; j++ {
			member := CreateValue(j)
			err := client.SAdd(key, member).Err()
			if err != nil && !smembers.config.IgnoreErrors {
				panic(err)
			}
		}
	}
}

func (smembers *SmembersBenchmark) Cleanup() {
}

func (smembers *SmembersBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (smembers *SmembersBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, value string) {
	executionStartTime := time.Now()

	err := client.SMembers(key).Err()
	if err != nil && !smembers.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "smembers",
		Latency:   time.Now().Sub(executionStartTime),
	}
}

