TEST_PATTERN ?= Test

.PHONY: test
test:
	go test -v -run ${TEST_PATTERN}

.PHONY: dlv
dlv:
	#dlv test --build-flags="-gcflags '-N -l'" -- -test.v -test.run ${TEST_PATTERN}
	dlv test -- -test.v -test.run ${TEST_PATTERN}
