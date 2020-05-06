package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Processing test operations", func() {
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
})
