package filesystem

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAll(t *testing.T) {
	BeforeSuite(func() {})
	AfterSuite(func() {})

	RegisterFailHandler(Fail)
	RunSpecs(t, "Tests Suite")
}
