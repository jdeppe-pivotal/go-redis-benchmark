package benchmark

import (
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
	"strings"
	"sync"
	"time"
)

type Benchmark struct {
	TestNames           []string
	TestDistribution    []string
	TestConfig          *operations.TestConfig
	Runners             map[string]operations.Runner
	WorkChannel         chan *WorkUnit
	OperationLatencies  map[string]map[int]int
	ThroughputResults   map[string]*operations.ThroughputResult
	ResultCount         *int32
	ExpectedResultCount *int32
	WorkCompleted       bool
	WaitGroup           *sync.WaitGroup
	Logger              *log.Logger
	Writer              io.Writer
	RawPercentiles      bool
}

type WorkUnit struct {
	Id        int
	Operation string
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
		TestNames:           testNames,
		TestDistribution:    testDistribution,
		TestConfig:          testConfig,
		Runners:             runners,
		WorkChannel:         make(chan *WorkUnit, testConfig.ClientCount),
		OperationLatencies:  latencies,
		ThroughputResults:   throughput,
		ResultCount:         new(int32),
		ExpectedResultCount: new(int32),
		WorkCompleted:       false,
		WaitGroup:           &sync.WaitGroup{},
		Logger:              log.New(os.Stdout, "rbm", log.LstdFlags),
		Writer:              os.Stdout,
		RawPercentiles:      rawPercentiles,
	}

	// set up signal handler for CTRL-C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan

		bench.WaitGroup.Done()
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

func (bm *Benchmark) SetWriter(writer io.Writer) {
	bm.Writer = writer
	bm.Logger.SetOutput(writer)
}

func (bm *Benchmark) Launch() {
	bm.flushAll()

	bm.WaitGroup.Add(1)
	go bm.processResults()

	for i := 0; i < bm.TestConfig.ClientCount; i++ {
		host := bm.TestConfig.HostPort[i%len(bm.TestConfig.HostPort)]
		bm.WaitGroup.Add(1)
		go bm.consumeWork(host)
	}

	go bm.ProduceWork()

	bm.WaitGroup.Wait()

	// Do cleanup
	for _, testName := range bm.TestNames {
		bm.Runners[testName].Cleanup()
	}
}

func (bm *Benchmark) flushAll() {
	if bm.TestConfig.Flush {
		fmt.Fprint(bm.Writer, "Flushing all...")
		client := redis.NewClient(&redis.Options{
			Addr:        bm.TestConfig.HostPort[0],
			Password:    bm.TestConfig.Password,
			ReadTimeout: time.Duration(60 * time.Second),
		})
		err := client.FlushAll().Err()
		if err != nil {
			panic(fmt.Sprintf("error calling FLUSHALL: %s", err.Error()))
		}
		client.Close()

		fmt.Fprintln(bm.Writer, "Done!")
	}
}

func (bm *Benchmark) processResults() {
	var lateMap map[int]int
	var throughputResult *operations.ThroughputResult
	var elapsedTime uint64 = 0
	ticker := time.NewTicker(1 * time.Second)

	defer bm.WaitGroup.Done()

	for {
		select {
		case r := <-bm.TestConfig.Results:
			lateMap = bm.OperationLatencies[r.Operation]
			lateMap[int(r.Latency.Milliseconds())+1]++
			throughputResult = bm.ThroughputResults[r.Operation]
			throughputResult.OperationCount++
			throughputResult.ElapsedTime += uint64(r.Latency.Nanoseconds())
			elapsedTime += uint64(r.Latency.Nanoseconds())

			*bm.ResultCount++
			if bm.WorkCompleted && *bm.ResultCount == *bm.ExpectedResultCount {
				return
			}
		case <-ticker.C:
			log.Printf("-> %0.2f ops/sec (queued: %d)\n", float64(*bm.ResultCount)/(float64(elapsedTime)/1e9)*float64(bm.TestConfig.ClientCount), len(bm.WorkChannel))
		}
	}
}

func (bm *Benchmark) consumeWork(hostPort string) {
	var randInt = rand.New(rand.NewSource(time.Now().UnixNano()))
	client := redis.NewClient(&redis.Options{
		Addr: hostPort,
		Password: bm.TestConfig.Password,
	})

	for work := range bm.WorkChannel {
		randomKey := operations.CreateKey(randInt.Intn(bm.TestConfig.Variant1))
		randomValue := operations.CreateValue(randInt.Intn(bm.TestConfig.Variant2))
		bm.Runners[work.Operation].DoOneOperation(client, bm.TestConfig.Results, randomKey, randomValue)
	}

	bm.WaitGroup.Done()
}

func (bm *Benchmark) ProduceWork() {
	var randInt = rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < bm.TestConfig.Iterations; i++ {
		randomTestIndex := randInt.Intn(len(bm.TestDistribution))
		*bm.ExpectedResultCount += bm.Runners[bm.TestDistribution[randomTestIndex]].ResultsPerOperation()

		bm.WorkChannel <- &WorkUnit{
			Id:        i,
			Operation: bm.TestDistribution[randomTestIndex],
		}
	}

	bm.WorkCompleted = true
	close(bm.WorkChannel)
}

func (bm *Benchmark) PrintSummary() {
	for operation, lateMap := range bm.OperationLatencies {
		fmt.Fprintln(bm.Writer)
		fmt.Fprintf(bm.Writer, "Latencies for: %s\n", operation)
		fmt.Fprintln(bm.Writer, "============================")
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

		if bm.RawPercentiles {
			sort.Sort(sort.Reverse(sort.IntSlice(keys)))

			remainingSummed := summedValues
			for _, k := range keys {
				percent := (float64(remainingSummed) / float64(summedValues)) * 100
				fmt.Fprintf(bm.Writer, "%8.3f%% <= %4d ms  (%d/%d)\n", percent, k, remainingSummed, summedValues)
				remainingSummed -= lateMap[k]
			}
		} else {
			percentiles := []int{100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 85, 80}
			for _, p := range percentiles {
				value, position := bm.PercentileValue(p, ascendingValues)
				fmt.Fprintf(bm.Writer, "% 4d%% <= %5.1f ms  (%d/%d)\n", p, value, position, summedValues)
			}
		}

	}

	for operation, throughputResult := range bm.ThroughputResults {
		elapsedTimeSeconds := float64(throughputResult.ElapsedTime) / 1e9 / float64(bm.TestConfig.ClientCount)
		throughputSec := float64(throughputResult.OperationCount) / elapsedTimeSeconds

		fmt.Fprintln(bm.Writer)
		fmt.Fprintf(bm.Writer, "Summary for: %s\n", operation)
		fmt.Fprintln(bm.Writer, "============================")
		fmt.Fprintf(bm.Writer, "Throughput: %0.2f ops/sec\n", throughputSec)
		fmt.Fprintf(bm.Writer, "Operations: %d\n", throughputResult.OperationCount)
		fmt.Fprintf(bm.Writer, "Elapsed time: %0.3f seconds\n", elapsedTimeSeconds)
	}

	fmt.Fprintln(bm.Writer)
	fmt.Fprintf(bm.Writer, "Clients:    %d\n", bm.TestConfig.ClientCount)
	fmt.Fprintf(bm.Writer, "Operations: %d\n", *bm.ResultCount)
	fmt.Fprintf(bm.Writer, "Variant1:   %d\n", bm.TestConfig.Variant1)
	fmt.Fprintf(bm.Writer, "Variant2:   %d\n", bm.TestConfig.Variant2)
	fmt.Fprintln(bm.Writer)
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
