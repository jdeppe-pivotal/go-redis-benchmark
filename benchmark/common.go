package benchmark

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type TestConfig struct {
	HostPort            []string
	ClientCount         int
	Iterations          int
	Variant1            int
	Variant2            int
	IgnoreErrors        bool
	Churn               bool
	Results             chan time.Duration
}

type Runner interface {
	Setup()
	DoOneOperation(client *redis.Client, results chan time.Duration)
	Cleanup()
	ResultsPerOperation() int32
}

