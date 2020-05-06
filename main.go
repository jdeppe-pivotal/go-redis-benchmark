package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"rbm/benchmark"
	"sort"
	"strings"
	"sync"
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
	runners             map[string]benchmark.Runner
	workChannel         chan *WorkUnit
	latencies           map[string]map[int]int
	throughput          map[string]*benchmark.ThroughputResult
	resultCount         *int32
	expectedResultCount *int32
	workCompleted       bool
	waitGroup           *sync.WaitGroup
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
	var flush bool
	var help bool
	var ignoreErrors bool
	var churn bool
	var bulk bool

	flag.StringVar(&hostsPorts, "h", HOST_PORT, "comma-separated host:port list")
	flag.IntVar(&iterations, "i", ITERATIONS, "iterations of the test to run - divided among clients")
	flag.IntVar(&clientCount, "c", CLIENT_COUNT, "number of clients to use")
	flag.IntVar(&variant1, "x", 1, `variant 1 - test dependent.
  sadd: the range of sets to use
  srem: the range of sets to use
  smembers: the range of sets to use
  del: the range of sets to use
  pubsub: the number of subscribers and -c should be used for publishers`)
	flag.IntVar(&variant2, "y", 1, `variant 2 - test dependent.
  sadd: the range of random member names to add
  srem: the number of elements to add to each set
  smembers: the number of elements to add to each set
  del: the number of entries to create in a set before deleting it`)
	flag.StringVar(&testName, "t", "sadd", "benchmark to run: del, ping, pubsub, sadd, setOperations, smembers, srem")
	flag.BoolVar(&flush, "flush", true, "flush before starting the benchmark run")
	flag.BoolVar(&help, "help", false, "help")
	flag.BoolVar(&ignoreErrors, "ignore-errors", false, "ignore errors from Redis calls")
	flag.BoolVar(&bulk, "bulk", false, "sadd and srem will be given multiple members to add/remove based on -y option")
	flag.BoolVar(&churn, "churn", false, "delete entries immediately after creation by sadd benchmark")

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
		Flush:        flush,
		IgnoreErrors: ignoreErrors,
		Churn:        churn,
		Bulk:         bulk,
		Results:      make(chan *benchmark.OperationResult, clientCount*2),
	}
}

func (bm *Benchmark) launch() {
	bm.flushAll()

	bm.waitGroup.Add(1)
	go bm.processResults()

	for i := 0; i < bm.testConfig.ClientCount; i++ {
		host := bm.testConfig.HostPort[i%len(bm.testConfig.HostPort)]
		bm.waitGroup.Add(1)
		go bm.consumeWork(host)
	}

	go bm.produceWork()

	bm.waitGroup.Wait()

	// Do cleanup
	bm.runners[bm.testName].Cleanup()
}

func (bm *Benchmark) flushAll() {
	if bm.testConfig.Flush {
		fmt.Printf("Flushing all!")
		client := redis.NewClient(&redis.Options{
			Addr:        bm.testConfig.HostPort[0],
			ReadTimeout: time.Duration(60 * time.Second),
		})
		err := client.FlushAll().Err()
		if err != nil {
			panic(fmt.Sprintf("error calling FLUSHALL: %s", err.Error()))
		}
		client.Close()
	}
}

func NewBenchmark(testName string, testConfig *benchmark.TestConfig) *Benchmark {
	var runner benchmark.Runner

	latencies := make(map[string]map[int]int)
	throughput := make(map[string]*benchmark.ThroughputResult)

	switch testName {
	case "ping":
		runner = benchmark.NewPingBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)

		break
	case "sadd":
		runner = benchmark.NewSaddBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)

		break
	case "smembers":
		runner = benchmark.NewSmembersBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)

		break
	case "srem":
		runner = benchmark.NewSremBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)
		// Because srem also does sadds
		latencies["sadd"] = make(map[int]int)
		throughput["sadd"] = new(benchmark.ThroughputResult)
		break
	case "setOperations":
		runner = benchmark.NewSetOperationsBenchmark(testConfig)
		latencies["sadd"] = make(map[int]int)
		throughput["sadd"] = new(benchmark.ThroughputResult)

		latencies["srem"] = make(map[int]int)
		throughput["srem"] = new(benchmark.ThroughputResult)

		latencies["smembers"] = make(map[int]int)
		throughput["smembers"] = new(benchmark.ThroughputResult)

		latencies["del"] = make(map[int]int)
		throughput["del"] = new(benchmark.ThroughputResult)
		break
	case "del":
		runner = benchmark.NewDelBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)

		// Because del also does sadds
		latencies["sadd"] = make(map[int]int)
		throughput["sadd"] = new(benchmark.ThroughputResult)
		break
	case "pubsub":
		runner = benchmark.NewPubSubBenchmark(testConfig)
		latencies[testName] = make(map[int]int)
		throughput[testName] = new(benchmark.ThroughputResult)
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
		workChannel:         make(chan *WorkUnit, testConfig.ClientCount),
		latencies:           latencies,
		throughput:          throughput,
		resultCount:         new(int32),
		expectedResultCount: new(int32),
		workCompleted:       false,
		waitGroup:           &sync.WaitGroup{},
	}

	// set up signal handler for CTRL-C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan

		bench.waitGroup.Done()
		bench.printSummary()

		os.Exit(0)
	}()

	return bench
}

func (bm *Benchmark) processResults() {
	var lateMap map[int]int
	var throughputResult *benchmark.ThroughputResult
	var elapsedTime uint64 = 0
	ticker := time.NewTicker(1 * time.Second)

	defer bm.waitGroup.Done()

	for {
		select {
		case r := <-bm.testConfig.Results:
			lateMap = bm.latencies[r.Operation]
			lateMap[int(r.Latency.Milliseconds())+1]++
			throughputResult = bm.throughput[r.Operation]
			throughputResult.OperationCount++
			throughputResult.ElapsedTime += uint64(r.Latency.Nanoseconds())
			elapsedTime += uint64(r.Latency.Nanoseconds())

			*bm.resultCount++
			if bm.workCompleted && *bm.resultCount == *bm.expectedResultCount {
				return
			}
		case <-ticker.C:
			log.Printf("-> %0.2f ops/sec (queued: %d)\n", float64(*bm.resultCount)/(float64(elapsedTime)/1e9)*float64(bm.testConfig.ClientCount), len(bm.workChannel))
		}
	}
}

func (bm *Benchmark) consumeWork(hostPort string) {
	var randInt = rand.New(rand.NewSource(time.Now().UnixNano()))
	client := redis.NewClient(&redis.Options{
		Addr: hostPort,
	})

	for work := range bm.workChannel {
		randomKey := benchmark.CreateKey(randInt.Intn(bm.testConfig.Variant1))
		randomValue := benchmark.CreateValue(randInt.Intn(bm.testConfig.Variant2))
		bm.runners[work.operation].DoOneOperation(client, bm.testConfig.Results, randomKey, randomValue)
	}

	bm.waitGroup.Done()
}

func (bm *Benchmark) produceWork() {

	for i := 0; i < bm.testConfig.Iterations; i++ {
		*bm.expectedResultCount += bm.runners[bm.testName].ResultsPerOperation()

		bm.workChannel <- &WorkUnit{
			id:        i,
			operation: bm.testName,
		}
	}

	bm.workCompleted = true
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
		fmt.Println()
	}

	for operation, throughputResult := range bm.throughput {
		elapsedTimeSeconds := float64(throughputResult.ElapsedTime) / 1e9 / float64(bm.testConfig.ClientCount)
		fmt.Printf("Elapsed time: %0.3f seconds\n", elapsedTimeSeconds)
		fmt.Printf("Operations: %d\n", throughputResult.OperationCount)
		throughputSec := float64(throughputResult.OperationCount) / elapsedTimeSeconds

		fmt.Printf("Throughput for %s: %0.2f ops/sec\n", operation, throughputSec)
	}

	fmt.Println()
	fmt.Printf("Clients:    %d\n", bm.testConfig.ClientCount)
	fmt.Printf("Operations: %d\n", *bm.resultCount)
	fmt.Printf("Variant1:   %d\n", bm.testConfig.Variant1)
	fmt.Printf("Variant2:   %d\n", bm.testConfig.Variant2)
	fmt.Println()
}
