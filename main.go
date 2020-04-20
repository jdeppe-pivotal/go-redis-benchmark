package main

import (
	"flag"
	"fmt"
	"rbm/benchmark"
	"strings"
)

const (
	ITERATIONS   = 100000
	HOST_PORT    = "localhost:6379"
	CLIENT_COUNT = 50
)

func main() {
	var hostsPortsArg string
	var iterations int
	var clientCount int
	var variant1 int
	var variant2 int
	var testName string
	var help bool
	var ignoreErrors bool
	var churn bool

	flag.StringVar(&hostsPortsArg, "h", HOST_PORT, "comma-separated host:port list")
	flag.IntVar(&iterations, "i", ITERATIONS, "iterations of the test to run - divided among clients")
	flag.IntVar(&clientCount, "c", CLIENT_COUNT, "number of clients to use")
	flag.IntVar(&variant1, "x", 1, "variant 1 - test dependent")
	flag.IntVar(&variant2, "y", 1, "variant 2 - test dependent")
	flag.StringVar(&testName, "t", "sadd", "benchmark to run")
	flag.BoolVar(&help, "help", false, "help")
	flag.BoolVar(&ignoreErrors, "ignore-errors", false, "ignore errors from Redis calls")
	flag.BoolVar(&churn, "churn", false, "delete entries immediately after creation")

	flag.Parse()

	if help {
		flag.Usage()
	}

	hostsPorts := strings.Split(hostsPortsArg, ",")

	clientIterations := iterations / clientCount

	testConfig := &benchmark.TestConfig{
		HostPort:         hostsPorts,
		ClientCount:      clientCount,
		Iterations:       iterations,
		ClientIterations: clientIterations,
		Variant1:         variant1,
		Variant2:         variant2,
		IgnoreErrors:     ignoreErrors,
		Churn:            churn,
	}

	var benchmarker benchmark.Runner

	switch testName {
	case "sadd":
		benchmarker = benchmark.NewSaddBenchmark(testConfig)
		break
	case "pubsub":
		benchmarker = benchmark.NewPubSubBenchmark(testConfig)
		break
	default:
		panic(fmt.Sprintf("unknown test: %s", testName))
	}

	//benchmarker.Execute()
	benchmarker.Launch()
}
