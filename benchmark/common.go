package benchmark

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type TestConfig struct {
	HostPort     []string
	ClientCount  int
	Iterations   int
	Variant1     int
	Variant2     int
	IgnoreErrors bool
	Churn        bool
	Results      chan *OperationResult
}

type OperationResult struct {
	Latency   time.Duration
	Operation string
}

type ThroughputResult struct {
	OperationCount int
	ElapsedTime int64
}

type Runner interface {
	Setup()
	DoOneOperation(client *redis.Client, results chan *OperationResult)
	Cleanup()
	ResultsPerOperation() int32
}
