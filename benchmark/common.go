package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"time"
)

type TestConfig struct {
	HostPort     []string
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
	OperationCount int
	ElapsedTime    uint64
}

type Runner interface {
	Setup()
	DoOneOperation(client *redis.Client, results chan *OperationResult)
	Cleanup()
	ResultsPerOperation() int32
}

func CreateKey(i int) string {
	return fmt.Sprintf("myKey-%05d", i)
}

func CreateValue(i int) string {
	return fmt.Sprintf("myValue-%010d", i)
}