package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoRedisBenchmark(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoRedisBenchmark Suite")
}
