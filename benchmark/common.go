package benchmark

import (
	"fmt"
	"log"
	"sort"
	"time"
)

type TestConfig struct {
	HostPort         []string
	ClientCount      int
	Iterations       int
	ClientIterations int
	Variant1         int
	Variant2         int
	IgnoreErrors     bool
	Churn            bool
}

type Runner interface {
	Launch()
}

func printSummary(latencies map[int]int) {
	var keys []int
	summedValues := 0
	for k, v := range latencies {
		keys = append(keys, k)
		summedValues += v
	}

	sort.Sort(sort.Reverse(sort.IntSlice(keys)))

	remainingSummed := summedValues
	for _, k := range keys {
		percent := (float64(remainingSummed) / float64(summedValues)) * 100
		fmt.Printf("%8.3f%% <= %4d ms  (%d/%d)\n", percent, k, remainingSummed, summedValues)
		remainingSummed -= latencies[k]
	}
}

func throughputTicker(value *int, quitter chan struct{}) {
	lastResultCount := 0
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			resultsNow := *value
			log.Printf("-> %d ops/sec\n", resultsNow-lastResultCount)
			lastResultCount = resultsNow
		case <-quitter:
			ticker.Stop()
			return
		}
	}
}
