package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis/v7"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"rbm/operations"
	"sort"
	"strconv"
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
	testNames           []string
	testDistribution    []string
	testConfig          *operations.TestConfig
	runners             map[string]operations.Runner
	workChannel         chan *WorkUnit
	operationLatencies  map[string]map[int]int
	throughputResults   map[string]*operations.ThroughputResult
	resultCount         *int32
	expectedResultCount *int32
	workCompleted       bool
	waitGroup           *sync.WaitGroup
	logger              *log.Logger
	rawPercentiles      bool
}

func main() {
	testOpDistribution, testConfig, rawPercentiles := processOptions(os.Args[1:])

	bm := NewBenchmark(testOpDistribution, testConfig, rawPercentiles)
	bm.Launch()

	bm.PrintSummary()
}

func processOptions(args []string) (map[string]int, *operations.TestConfig, bool) {
	var hostsPorts string
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
	var rawPercentiles bool

	flagSet := flag.NewFlagSet("rbm", flag.ExitOnError)

	flagSet.StringVar(&hostsPorts, "h", HOST_PORT, "comma-separated host:port list")
	flagSet.IntVar(&iterations, "i", ITERATIONS, "iterations of the test to run - divided among clients")
	flagSet.IntVar(&clientCount, "c", CLIENT_COUNT, "number of clients to use")
	flagSet.IntVar(&variant1, "x", 1, `variant 1 - test dependent.
  sadd: the range of sets to use
  srem: the range of sets to use
  smembers: the range of sets to use
  del: the range of sets to use
  pubsub: the number of subscribers and -c should be used for publishers`)
	flagSet.IntVar(&variant2, "y", 1, `variant 2 - test dependent.
  sadd: the range of random member names to add
  srem: the number of elements to add to each set
  smembers: the number of elements to add to each set
  del: the number of entries to create in a set before deleting it`)
	flagSet.StringVar(&testNames, "t", "sadd", `comma-separated list of benchmark to run: del, ping, pubsub, sadd, setOperations, smembers, srem
  Each test can also be assigned a ratio. For example 'sadd:4,smembers:1' will randomly run sadd and smembers operations with a respective proportion of 4:1`)
	flagSet.BoolVar(&flush, "flush", true, "flush before starting the benchmark run")
	flagSet.BoolVar(&help, "help", false, "help")
	flagSet.BoolVar(&ignoreErrors, "ignore-errors", false, "ignore errors from Redis calls")
	flagSet.BoolVar(&bulk, "bulk", false, "sadd and srem will be given multiple members to add/remove based on -y option")
	flagSet.BoolVar(&churn, "churn", false, "delete entries immediately after creation by sadd benchmark")
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
		ClientCount:  clientCount,
		Iterations:   iterations,
		Variant1:     variant1,
		Variant2:     variant2,
		Flush:        flush,
		IgnoreErrors: ignoreErrors,
		Churn:        churn,
		Bulk:         bulk,
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

func NewBenchmark(testOpDistribution map[string]int, testConfig *operations.TestConfig, rawPercentiles bool) *Benchmark {
	var runner operations.Runner

	latencies := make(map[string]map[int]int)
	throughput := make(map[string]*operations.ThroughputResult)
	runners := make(map[string]operations.Runner, 0)

	for testName, _ := range testOpDistribution {
		switch testName {
		case "ping":
			runner = operations.NewPingBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)

			break
		case "sadd":
			runner = operations.NewSaddBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)

			break
		case "smembers":
			runner = operations.NewSmembersBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)

			break
		case "srem":
			runner = operations.NewSremBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)
			// Because srem also does sadds
			latencies["sadd"] = make(map[int]int)
			throughput["sadd"] = new(operations.ThroughputResult)
			break
		case "del":
			runner = operations.NewDelBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)

			// Because del also does sadds
			latencies["sadd"] = make(map[int]int)
			throughput["sadd"] = new(operations.ThroughputResult)
			break
		case "pubsub":
			runner = operations.NewPubSubBenchmark(testConfig)
			latencies[testName] = make(map[int]int)
			throughput[testName] = new(operations.ThroughputResult)
			break
		default:
			if strings.HasPrefix(testName, "fakeTest") {
				runner = operations.NewFakeBenchmark(testConfig)
				latencies[testName] = make(map[int]int)
				throughput[testName] = new(operations.ThroughputResult)
			} else {
				panic(fmt.Sprintf("unknown test: %s", testName))
			}
		}

		runner.Setup()
		runners[testName] = runner
	}

	testNames, testDistribution := makeTestOpDistributions(testOpDistribution)

	bench := &Benchmark{
		testNames:           testNames,
		testDistribution:    testDistribution,
		testConfig:          testConfig,
		runners:             runners,
		workChannel:         make(chan *WorkUnit, testConfig.ClientCount),
		operationLatencies:  latencies,
		throughputResults:   throughput,
		resultCount:         new(int32),
		expectedResultCount: new(int32),
		workCompleted:       false,
		waitGroup:           &sync.WaitGroup{},
		logger:              log.New(os.Stdout, "rbm", log.LstdFlags),
		rawPercentiles:      rawPercentiles,
	}

	// set up signal handler for CTRL-C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan

		bench.waitGroup.Done()
		bench.PrintSummary()

		os.Exit(0)
	}()

	return bench
}

func makeTestOpDistributions(testOpDistribution map[string]int) ([]string, []string) {
	testNames := make([]string, 0, len(testOpDistribution))
	distribution := make([]string, 0)

	for name, count := range testOpDistribution {
		testNames = append(testNames, name)

		for i := 0; i < count; i++ {
			distribution = append(distribution, name)
		}
	}

	return testNames, distribution
}

func (bm *Benchmark) SetLogWriter(writer io.Writer) {
	bm.logger.SetOutput(writer)
}

func (bm *Benchmark) Launch() {
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
	for _, testName := range bm.testNames {
		bm.runners[testName].Cleanup()
	}
}

func (bm *Benchmark) flushAll() {
	if bm.testConfig.Flush {
		fmt.Print("Flushing all...")
		client := redis.NewClient(&redis.Options{
			Addr:        bm.testConfig.HostPort[0],
			ReadTimeout: time.Duration(60 * time.Second),
		})
		err := client.FlushAll().Err()
		if err != nil {
			panic(fmt.Sprintf("error calling FLUSHALL: %s", err.Error()))
		}
		client.Close()

		fmt.Println("Done!")
	}
}

func (bm *Benchmark) processResults() {
	var lateMap map[int]int
	var throughputResult *operations.ThroughputResult
	var elapsedTime uint64 = 0
	ticker := time.NewTicker(1 * time.Second)

	defer bm.waitGroup.Done()

	for {
		select {
		case r := <-bm.testConfig.Results:
			lateMap = bm.operationLatencies[r.Operation]
			lateMap[int(r.Latency.Milliseconds())+1]++
			throughputResult = bm.throughputResults[r.Operation]
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
		randomKey := operations.CreateKey(randInt.Intn(bm.testConfig.Variant1))
		randomValue := operations.CreateValue(randInt.Intn(bm.testConfig.Variant2))
		bm.runners[work.operation].DoOneOperation(client, bm.testConfig.Results, randomKey, randomValue)
	}

	bm.waitGroup.Done()
}

func (bm *Benchmark) produceWork() {
	var randInt = rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < bm.testConfig.Iterations; i++ {
		randomTestIndex := randInt.Intn(len(bm.testDistribution))
		*bm.expectedResultCount += bm.runners[bm.testDistribution[randomTestIndex]].ResultsPerOperation()

		bm.workChannel <- &WorkUnit{
			id:        i,
			operation: bm.testDistribution[randomTestIndex],
		}
	}

	bm.workCompleted = true
	close(bm.workChannel)
}

func (bm *Benchmark) PrintSummary() {
	for operation, lateMap := range bm.operationLatencies {
		fmt.Println()
		fmt.Printf("Latencies for: %s\n", operation)
		fmt.Println("============================")
		var keys []int
		summedValues := 0
		for k, v := range lateMap {
			keys = append(keys, k)
			summedValues += v
		}

		sort.Sort(sort.IntSlice(keys))
		ascendingValues := make([]int, summedValues)
		idx := 0
		for _, k := range keys {
			for i := 0; i < lateMap[k]; i++ {
				ascendingValues[idx] = k
				idx++
			}
		}

		if bm.rawPercentiles {
			sort.Sort(sort.Reverse(sort.IntSlice(keys)))

			remainingSummed := summedValues
			for _, k := range keys {
				percent := (float64(remainingSummed) / float64(summedValues)) * 100
				fmt.Printf("%8.3f%% <= %4d ms  (%d/%d)\n", percent, k, remainingSummed, summedValues)
				remainingSummed -= lateMap[k]
			}
		} else {
			percentiles := []int{100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 85, 80}
			for _, p := range percentiles {
				value, position := bm.PercentileValue(p, ascendingValues)
				fmt.Printf("% 4d%% <= %5.1f ms  (%d/%d)\n", p, value, position, summedValues)
			}
		}

	}

	for operation, throughputResult := range bm.throughputResults {
		elapsedTimeSeconds := float64(throughputResult.ElapsedTime) / 1e9 / float64(bm.testConfig.ClientCount)
		throughputSec := float64(throughputResult.OperationCount) / elapsedTimeSeconds

		fmt.Println()
		fmt.Printf("Summary for: %s\n", operation)
		fmt.Println("============================")
		fmt.Printf("Throughput: %0.2f ops/sec\n", throughputSec)
		fmt.Printf("Operations: %d\n", throughputResult.OperationCount)
		fmt.Printf("Elapsed time: %0.3f seconds\n", elapsedTimeSeconds)
	}

	fmt.Println()
	fmt.Printf("Clients:    %d\n", bm.testConfig.ClientCount)
	fmt.Printf("Operations: %d\n", *bm.resultCount)
	fmt.Printf("Variant1:   %d\n", bm.testConfig.Variant1)
	fmt.Printf("Variant2:   %d\n", bm.testConfig.Variant2)
	fmt.Println()
}

func (bm *Benchmark) PercentileValue(percentile int, sortedData []int) (float64, int) {
	if percentile == 100 {
		return float64(sortedData[len(sortedData)-1]), len(sortedData)
	}

	position := float64(len(sortedData)) * (float64(percentile) / 100)
	intPosition := int(math.Ceil(position))

	if intPosition == int(position) {
		return float64(sortedData[intPosition]), intPosition
	} else {
		return float64(sortedData[intPosition]+sortedData[intPosition-1]) / 2, intPosition
	}
}
