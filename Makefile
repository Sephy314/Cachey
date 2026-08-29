.PHONY: test vet race check fmt build-harness

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

check: test vet

fmt:
	go fmt ./...

build-harness:
	go build ./cmd/harness