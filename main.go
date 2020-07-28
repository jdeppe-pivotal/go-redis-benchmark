package main

import (
	"flag"
	"os"
	"rbm/benchmark"
	"rbm/operations"
	"strconv"
	"strings"
)

const (
	ITERATIONS   = 100000
	HOST_PORT    = "localhost:6379"
	CLIENT_COUNT = 50
)

func main() {
	testOpDistribution, testConfig, rawPercentiles := processOptions(os.Args[1:])

	bm := benchmark.NewBenchmark(testOpDistribution, testConfig, rawPercentiles)
	bm.Launch()

	bm.PrintSummary()
}

func processOptions(args []string) (map[string]int, *operations.TestConfig, bool) {
	var hostsPorts string
	var password string
	var iterations int
	var clientCount int
	var variant1 int
	var variant2 int
	var testNames string
	var flush bool
	var help bool
	var ignoreErrors bool
	var churn bool
	var bulk bool
	var load bool
	var rawPercentiles bool
	var useTls bool

	flagSet := flag.NewFlagSet("rbm", flag.ExitOnError)

	flagSet.StringVar(&hostsPorts, "h", HOST_PORT, "comma-separated host:port list")
	flagSet.StringVar(&password, "p", "", "password")
	flagSet.IntVar(&iterations, "i", ITERATIONS, "iterations of the test to run - divided among clients")
	flagSet.IntVar(&clientCount, "c", CLIENT_COUNT, "number of clients to use")
	flagSet.IntVar(&variant1, "x", 1, `variant 1 - test dependent.
  hgetall: the range of hashes to use
  hset: the range of hashes to use
  sadd: the range of sets to use
  srem: the range of sets to use
  smembers: the range of sets to use
  del: the range of sets to use
  pubsub: the number of subscribers and -c should be used for publishers`)
	flagSet.IntVar(&variant2, "y", 1, `variant 2 - test dependent.
  hset: used for both the range of fields and values
  sadd: the range of random member names to add
  srem: the number of elements to add to each set
  smembers: the number of elements to add to each set
  del: the number of entries to create in a set before deleting it`)
	flagSet.StringVar(&testNames, "t", "sadd", `comma-separated list of benchmark to run: del, echo, get, hgetall, hset, ping, pubsub, sadd, set, setOperations, smembers, srem
  Each test can also be assigned a ratio. For example 'sadd:4,smembers:1' will randomly run sadd and smembers operations with a respective proportion of 4:1`)
	flagSet.BoolVar(&flush, "flush", true, "flush before starting the benchmark run")
	flagSet.BoolVar(&help, "help", false, "help")
	flagSet.BoolVar(&ignoreErrors, "ignore-errors", false, "ignore errors from Redis calls")
	flagSet.BoolVar(&bulk, "bulk", false, "sadd and srem will be given multiple members to add/remove based on -y option")
	flagSet.BoolVar(&churn, "churn", false, "delete entries immediately after creation by sadd benchmark")
	flagSet.BoolVar(&load, "load", false, "pre-load data before running the test. This is operation dependent, but will typically use the value for -x to determine how much data to load.")
	flagSet.BoolVar(&useTls, "tls", false, "enable TLS for connections.")
	flagSet.BoolVar(&rawPercentiles, "raw-percentiles", false, "show raw percentiles instead of a fixed (and calculated) set")

	flagSet.Parse(args)

	if help {
		flagSet.Usage()
		os.Exit(0)
	}

	hostsPortsList := strings.Split(hostsPorts, ",")

	testOpDistribution := mapTestsToDistribution(testNames)

	testConfig := &operations.TestConfig{
		HostPort:     hostsPortsList,
		Password:     password,
		ClientCount:  clientCount,
		Iterations:   iterations,
		Variant1:     variant1,
		Variant2:     variant2,
		Flush:        flush,
		IgnoreErrors: ignoreErrors,
		Churn:        churn,
		Bulk:         bulk,
		Load:         load,
		Tls:          useTls,
		Results:      make(chan *operations.OperationResult, clientCount*2),
	}

	return testOpDistribution, testConfig, rawPercentiles
}

// Convert -t option such as 'sadd:3,srem:10' into an actual map
func mapTestsToDistribution(rawTestsArg string) map[string]int {
	proportions := make(map[string]int)
	tests := strings.Split(rawTestsArg, ",")
	var name string
	var ratio int
	var err error

	for _, nameAndRatio := range tests {
		if idx := strings.Index(nameAndRatio, ":"); idx > 0 {
			ratio, err = strconv.Atoi(nameAndRatio[idx+1:])
			if err != nil {
				panic("Unable to parse " + nameAndRatio)
			}
			name = nameAndRatio[:idx]
		} else {
			name = nameAndRatio
			ratio = 1
		}

		// Handle composite test names
		if name == "setOperations" {
			proportions["sadd"] = ratio
			proportions["srem"] = ratio
			proportions["smembers"] = ratio
			proportions["del"] = ratio
		} else {
			proportions[name] = ratio
		}
	}

	return proportions
}
