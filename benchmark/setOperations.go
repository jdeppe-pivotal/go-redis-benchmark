package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SetOperationsBenchmark struct {
	config  *TestConfig
	members []string
	randInt *rand.Rand
}

var _ Runner = (*SetOperationsBenchmark)(nil)

func NewSetOperationsBenchmark(config *TestConfig) Runner {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SetOperationsBenchmark{
		config: config,
		randInt: randInt,
	}
}

func (setOperations *SetOperationsBenchmark) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: setOperations.config.HostPort[0],
	})
	setOperations.members = make([]string, setOperations.config.Variant2)
	for j := 0; j < setOperations.config.Variant2; j++ {
		setOperations.members[j] = fmt.Sprintf("myValue-%d",j)
	}

	for i := 0; i < setOperations.config.Variant1; i++ {
		client.SAdd(fmt.Sprintf("mykey-%d",i), setOperations.members)
	}

	client.Close()
}

func (setOperations *SetOperationsBenchmark) Cleanup() {}

func (setOperations *SetOperationsBenchmark) ResultsPerOperation() int32 {
	return 2
}

func (setOperations *SetOperationsBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult) {
	operationIndex := setOperations.randInt.Intn(4)
	key := fmt.Sprintf("mykey-%d", setOperations.randInt.Intn(setOperations.config.Variant1))
	value := fmt.Sprintf("myValue-%d", setOperations.randInt.Intn(setOperations.config.Variant2))

	switch operationIndex {
	case 0:
		saddStart := time.Now()
		err = client.SAdd(key, value).Err()
		if err != nil && !srem.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "sadd",
			Latency:   time.Now().Sub(saddStart),
		}
		break
	}
	executionStartTime := time.Now()
	err := client.SRem(key, value).Err()
	if err != nil && !setOperations.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "srem",
		Latency:   time.Now().Sub(executionStartTime),
	}

	saddStart := time.Now()
	err = client.SAdd(key, value).Err()
	if err != nil && !srem.config.IgnoreErrors {
		panic(err)
	}

	results <- &OperationResult{
		Operation: "sadd",
		Latency:   time.Now().Sub(saddStart),
	}
}

