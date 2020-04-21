package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"os"
	"os/signal"
	"rbm/benchmark"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ITERATIONS   = 100000
	HOST_PORT    = "localhost:6379"
	CLIENT_COUNT = 50
)

type WorkUnit struct {
	id        int
	operation string
}

type Benchmark struct {
	testName            string
	testConfig          *benchmark.TestConfig
	startTime           time.Time
	endTime             time.Time
	runners             map[string]benchmark.Runner
	workChannel         chan *WorkUnit
	latencies           map[string]map[int]int
	resultCount         *int
	expectedResultCount *int32
	tickQuitter         chan bool
}

func main() {
	testName, testConfig := processOptions()

	bm := NewBenchmark(testName, testConfig)
	bm.launch()

	bm.printSummary()
}

func processOptions() (string, *benchmark.TestConfig) {
	var hostsPorts string
	var iterations int
	var clientCount int
	var variant1 int
	var variant2 int
	var testName string
	var help bool
	var ignoreErrors bool
	var churn bool

	flag.StringVar(&hostsPorts, "h", HOST_PORT, "comma-separated host:port list")
	flag.IntVar(&iterations, "i", ITERATIONS, "iterations of the test to run - divided among clients")
	flag.IntVar(&clientCount, "c", CLIENT_COUNT, "number of clients to use")
	flag.IntVar(&variant1, "x", 1, `variant 1 - test dependent.
  sadd: the range of sets to use
  smembers: the range of sets to use
  del: the range of sets to use
  pubsub: the number of subscribers and -c should be used for publishers`)
	flag.IntVar(&variant2, "y", 1, `variant 2 - test dependent.
  sadd: the range of random member names to add
  smembers: the number of elements to add to each set
  del: the number of entries to create in a set before deleting it`)
	flag.StringVar(&testName, "t", "sadd", "benchmark to run: sadd, smembers, del, pubsub")
	flag.BoolVar(&help, "help", false, "help")
	flag.BoolVar(&ignoreErrors, "ignore-errors", false, "ignore errors from Redis calls")
	flag.BoolVar(&churn, "churn", false, "delete entries immediately after creation")

	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(0)
	}

	hostsPortsList := strings.Split(hostsPorts, ",")

	return testName, &benchmark.TestConfig{
		HostPort:     hostsPortsList,
		ClientCount:  clientCount,
		Iterations:   iterations,
		Variant1:     variant1,
		Variant2:     variant2,
		IgnoreErrors: ignoreErrors,
		Churn:        churn,
		Results:      make(chan *benchmark.OperationResult),
	}
}

func (bm *Benchmark) launch() {
	go bm.throughputTicker(bm.resultCount, bm.tickQuitter)

	// Process results
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go bm.processResults(wg)

	for i := 0; i < bm.testConfig.ClientCount; i++ {
		host := bm.testConfig.HostPort[i%len(bm.testConfig.HostPort)]
		wg.Add(1)
		go bm.consumeWork(host, wg)
	}

	bm.startTime = time.Now()

	go bm.produceWork()

	wg.Wait()

	bm.endTime = time.Now()

	// Do cleanup
	bm.tickQuitter <- true
	bm.runners[bm.testName].Cleanup()
}

func NewBenchmark(testName string, testConfig *benchmark.TestConfig) *Benchmark {
	var runner benchmark.Runner

	latencies := make(map[string]map[int]int)
	latencies[testName] = make(map[int]int)

	switch testName {
	case "sadd":
		runner = benchmark.NewSaddBenchmark(testConfig)
		break
	case "smembers":
		runner = benchmark.NewSmembersBenchmark(testConfig)
		break
	case "del":
		runner = benchmark.NewDelBenchmark(testConfig)
		// Because del also does sadds
		latencies["sadd"] = make(map[int]int)
		break
	case "pubsub":
		runner = benchmark.NewPubSubBenchmark(testConfig)
		break
	default:
		panic(fmt.Sprintf("unknown test: %s", testName))
	}

	runner.Setup()

	runners := make(map[string]benchmark.Runner, 0)
	runners[testName] = runner

	bench := &Benchmark{
		testName:            testName,
		testConfig:          testConfig,
		runners:             runners,
		workChannel:         make(chan *WorkUnit, testConfig.ClientCount*2),
		latencies:           latencies,
		resultCount:         new(int),
		expectedResultCount: new(int32),
		tickQuitter:         make(chan bool),
	}

	// set up signal handler for CTRL-C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		bench.endTime = time.Now()
		bench.printSummary()
		os.Exit(0)
	}()

	return bench
}

func (bm *Benchmark) processResults(wg *sync.WaitGroup) {
	var lateMap map[int]int

	for r := range bm.testConfig.Results {
		lateMap = bm.latencies[r.Operation]
		lateMap[int(r.Latency.Milliseconds())+1]++

		*bm.resultCount++
		if *bm.resultCount == int(atomic.LoadInt32(bm.expectedResultCount)) {
			break
		}
	}

	wg.Done()
}

func (bm *Benchmark) consumeWork(hostPort string, wg *sync.WaitGroup) {
	client := redis.NewClient(&redis.Options{
		Addr: hostPort,
	})

	for work := range bm.workChannel {
		bm.runners[work.operation].DoOneOperation(client, bm.testConfig.Results)
	}

	wg.Done()
}

func (bm *Benchmark) produceWork() {
	for i := 0; i < bm.testConfig.Iterations; i++ {
		atomic.AddInt32(bm.expectedResultCount, bm.runners[bm.testName].ResultsPerOperation())
		bm.workChannel <- &WorkUnit{i, bm.testName}
	}

	close(bm.workChannel)
}

func (bm *Benchmark) printSummary() {

	for operation, lateMap := range bm.latencies {
		fmt.Println()
		fmt.Printf("Latencies for: %s\n", operation)
		fmt.Println("============================")
		var keys []int
		summedValues := 0
		for k, v := range lateMap {
			keys = append(keys, k)
			summedValues += v
		}

		sort.Sort(sort.Reverse(sort.IntSlice(keys)))

		remainingSummed := summedValues
		for _, k := range keys {
			percent := (float64(remainingSummed) / float64(summedValues)) * 100
			fmt.Printf("%8.3f%% <= %4d ms  (%d/%d)\n", percent, k, remainingSummed, summedValues)
			remainingSummed -= lateMap[k]
		}
	}

	throughput := float64(bm.testConfig.Iterations) / bm.endTime.Sub(bm.startTime).Seconds()

	fmt.Println()
	fmt.Printf("Clients:    %d\n", bm.testConfig.ClientCount)
	fmt.Printf("Operations: %d\n", *bm.resultCount)
	fmt.Printf("Variant1:   %d\n", bm.testConfig.Variant1)
	fmt.Printf("Variant2:   %d\n", bm.testConfig.Variant2)
	fmt.Printf("Throughput: %0.2f ops/sec\n", throughput)
	fmt.Println()
}

func (bm *Benchmark) throughputTicker(value *int, quitter chan bool) {
	lastResultCount := 0
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ticker.C:
			resultsNow := *value
			log.Printf("-> %d ops/sec (in flight: %d)\n", resultsNow-lastResultCount, int(*bm.expectedResultCount)-*bm.resultCount)
			lastResultCount = resultsNow
		case <-quitter:
			ticker.Stop()
			return
		}
	}
}
