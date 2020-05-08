package operations

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type TestConfig struct {
	HostPort     []string
	Password 	 string
	ClientCount  int
	Iterations   int
	Variant1     int
	Variant2     int
	Flush        bool
	IgnoreErrors bool
	Churn        bool
	Bulk         bool
	Results      chan *OperationResult
}

type OperationResult struct {
	Latency   time.Duration
	Operation string
}

type ThroughputResult struct {
	OperationCount  int
	AccumulatedTime uint64
}

type Runner interface {
	Setup()
	DoOneOperation(client *redis.Client, results chan *OperationResult, key string, value string)
	Cleanup()
	ResultsPerOperation() int32
}

var randInt = rand.New(rand.NewSource(time.Now().UnixNano()))

func CreateKey(i int) string {
	return fmt.Sprintf("myKey-%05d", i)
}

func CreateValue(i int) string {
	return fmt.Sprintf("myValue-%010d", i)
}
