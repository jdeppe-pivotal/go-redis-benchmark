package benchmark

import (
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
		config:  config,
		randInt: randInt,
	}
}

func (setOperations *SetOperationsBenchmark) Setup() {
	client := redis.NewClient(&redis.Options{
		Addr: setOperations.config.HostPort[0],
	})
	setOperations.members = make([]string, setOperations.config.Variant2)
	for j := 0; j < setOperations.config.Variant2; j++ {
		setOperations.members[j] = CreateValue(j)
	}

	for i := 0; i < setOperations.config.Variant1; i++ {
		client.SAdd(CreateKey(i), setOperations.members)
	}

	client.Close()
}

func (setOperations *SetOperationsBenchmark) Cleanup() {}

func (setOperations *SetOperationsBenchmark) ResultsPerOperation() int32 {
	return 1
}

func (setOperations *SetOperationsBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, value string) {
	operationIndex := setOperations.randInt.Intn(4)

	var err error

	switch operationIndex {
	case 0:
		saddStart := time.Now()
		if setOperations.config.Bulk {
			err = client.SAdd(key, setOperations.members).Err()
		} else {
			err = client.SAdd(key, value).Err()
		}
		if err != nil && !setOperations.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "sadd",
			Latency:   time.Now().Sub(saddStart),
		}
		break
	case 1:
		executionStartTime := time.Now()
		if setOperations.config.Bulk {
			err = client.SRem(key, setOperations.members).Err()
		} else {
			err = client.SRem(key, value).Err()
		}
		if err != nil && !setOperations.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "srem",
			Latency:   time.Now().Sub(executionStartTime),
		}
	case 2:
		executionStartTime := time.Now()

		err := client.SMembers(key).Err()
		if err != nil && !setOperations.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "smembers",
			Latency:   time.Now().Sub(executionStartTime),
		}
	case 3:
		executionStartTime := time.Now()
		err := client.Del(key).Err()
		if err != nil && !setOperations.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "del",
			Latency:   time.Now().Sub(executionStartTime),
		}
	default:
		panic("Unknown test index")
	}
}
