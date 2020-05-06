package benchmark_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"rbm/benchmark"
	"rbm/operations"
)

var _ = Describe("Benchmark test operations", func() {
	Context("NewBenchmark with single test", func() {
		It("Should contain a single value testDistribution", func() {
			testOpDistribution := map[string]int{"fakeTest": 1}
			bm := benchmark.NewBenchmark(testOpDistribution, &operations.TestConfig{}, false)

			Expect(bm.TestNames).To(ContainElement("fakeTest"))
			Expect(bm.TestDistribution).To(ContainElement("fakeTest"))
		})
	})

	Context("NewBenchmark with multiple tests", func() {
		It("Should contain multiple values in the testDistribution", func() {
			testOpDistribution := map[string]int{"fakeTestA": 1, "fakeTestB": 2, "fakeTestC": 3}
			bm := benchmark.NewBenchmark(testOpDistribution, &operations.TestConfig{}, false)

			Expect(bm.TestNames).To(ConsistOf("fakeTestC", "fakeTestA", "fakeTestB"))
			Expect(bm.TestDistribution).To(ConsistOf(
				"fakeTestA",
				"fakeTestB",
				"fakeTestB",
				"fakeTestC",
				"fakeTestC",
				"fakeTestC",
			))
		})
	})

	Context("produceWork with multiple tests", func() {
		It("Should produce random test distribution", func() {
			testOpDistribution := map[string]int{"fakeTestA": 1, "fakeTestB": 1}
			bm := benchmark.NewBenchmark(testOpDistribution, &operations.TestConfig{
				ClientCount: 1,
				Iterations: 10,
			}, false)

			workUnits := make([]string, 0, 10)

			go func() {
				bm.ProduceWork()
			}()

			for work := range bm.WorkChannel {
				workUnits = append(workUnits, work.Operation)
			}

			Expect(workUnits).To(ContainElement("fakeTestA"))
			Expect(workUnits).To(ContainElement("fakeTestB"))
		})
	})
})
