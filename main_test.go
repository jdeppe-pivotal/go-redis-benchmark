package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"rbm/benchmark"
)

var _ = Describe("Benchmark test operations", func() {
	Context("processOptions with single test operation", func() {
		It("Should produce a single test with distribution of 1", func() {
			args := []string{"-t", "sadd", "-h", "foo,bar"}
			testName, _, _ := processOptions(args)

			Expect(testName).To(Equal(map[string]int{"sadd": 1}))
		})
	})

	Context("processOptions with multiple test operations without distributions", func() {
		It("Should produce multiple tests with default distributions of 1", func() {
			args := []string{"-t", "sadd,srem"}
			testName, _, _ := processOptions(args)

			Expect(testName).To(Equal(map[string]int{"sadd": 1, "srem": 1}))
		})
	})

	Context("processOptions with multiple test operations with distributions", func() {
		It("Should produce multiple tests with correct distributions", func() {
			args := []string{"-t", "sadd:1,srem:3"}
			testName, _, _ := processOptions(args)

			Expect(testName).To(Equal(map[string]int{"srem": 3, "sadd": 1}))
		})
	})

	Context("processOptions with setOperation", func() {
		It("Should expand to correct tests with correct distributions", func() {
			args := []string{"-t", "setOperations:2"}
			testName, _, _ := processOptions(args)

			Expect(testName).To(Equal(map[string]int{"srem": 2, "sadd": 2, "smembers": 2, "del": 2}))
		})
	})

	Context("NewBenchmark with single test", func() {
		It("Should contain a single value testDistribution", func() {
			testOpDistribution := map[string]int{"fakeTest": 1}
			bm := NewBenchmark(testOpDistribution, &benchmark.TestConfig{}, false)

			Expect(bm.testNames).To(ContainElement("fakeTest"))
			Expect(bm.testDistribution).To(ContainElement("fakeTest"))
		})
	})

	Context("NewBenchmark with multiple tests", func() {
		It("Should contain multiple values in the testDistribution", func() {
			testOpDistribution := map[string]int{"fakeTestA": 1, "fakeTestB": 2, "fakeTestC": 3}
			bm := NewBenchmark(testOpDistribution, &benchmark.TestConfig{}, false)

			Expect(bm.testNames).To(ConsistOf("fakeTestC", "fakeTestA", "fakeTestB"))
			Expect(bm.testDistribution).To(ConsistOf(
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
			bm := NewBenchmark(testOpDistribution, &benchmark.TestConfig{
				ClientCount: 1,
				Iterations: 10,
			}, false)

			workUnits := make([]string, 0, 10)

			go func() {
				bm.produceWork()
			}()

			for work := range bm.workChannel {
				workUnits = append(workUnits, work.operation)
			}

			Expect(workUnits).To(ContainElement("fakeTestA"))
			Expect(workUnits).To(ContainElement("fakeTestB"))
		})
	})
})
