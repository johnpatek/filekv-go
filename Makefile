UNIT_TEST_HEADER        = "****************************** UNIT TEST *******************************"
INTEGRATION_TEST_HEADER = "*************************** INTEGRATION TEST ***************************"
LINT_TEST_HEADER        = "****************************** LINT TEST *******************************"
BENCHMARK_TEST_HEADER   = "**************************** BENCHMARK TEST ****************************"
CODE_COVERAGE_HEADER    = "**************************** CODE COVERAGE *****************************" 
.PHONY: all
all: test lint

.PHONY: test
test: unit cover integration benchmark

.PHONY: unit
unit:
	@echo $(UNIT_TEST_HEADER)
	@go test -v -timeout 30s -coverprofile=coverage.out github.com/johnpatek/filekv-go -skip TestFileKV

.PHONY: cover
cover:
	@echo $(CODE_COVERAGE_HEADER)
	@go tool cover -func=coverage.out
	
.PHONY: integration
integration:
	@echo $(INTEGRATION_TEST_HEADER)
	@go test -v -run TestFileKV

.PHONY: benchmark
benchmark:
	@echo $(BENCHMARK_TEST_HEADER)
	@go test -v -bench=. -run=BenchmarkFileKV

.PHONY: lint
lint:
	@echo $(LINT_TEST_HEADER)
	@if [ ! -f bin/golangci-lint ]; then \
    	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b bin; \
	fi
	@./bin/golangci-lint -v run pipelayer.go

.PHONY: clean
clean:
	rm -rf coverage.out