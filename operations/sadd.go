package operations

import (
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

func (sadd *SaddBenchmark) Setup(clients []*redis.Client) {
	if sadd.config.Bulk {
		sadd.members = make([]string, sadd.config.Variant2)
		for j := 0; j < sadd.config.Variant2; j++ {
			sadd.members[j] = CreateValue(j)
		}
	}
}

func (sadd *SaddBenchmark) Cleanup() {
}

func (sadd *SaddBenchmark) ResultsPerOperation() int32 {
	if sadd.config.Churn {
		return 2
	} else {
		return 1
	}
}

func (sadd *SaddBenchmark) DoOneOperation(client *redis.Client, results chan *OperationResult, key string, member string) {
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
		sremStartTime := time.Now()
		if sadd.config.Bulk {
			err = client.SRem(key, sadd.members).Err()
		} else {
			err = client.SRem(key, member).Err()
		}
		if err != nil && !sadd.config.IgnoreErrors {
			panic(err)
		}

		results <- &OperationResult{
			Operation: "srem",
			Latency:   time.Now().Sub(sremStartTime),
		}
	}

}
