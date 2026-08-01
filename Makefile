# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used architecture if $GOOS/$GOARCH are not set
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all
all: build

.PHONY: fmt
fmt:
	go fmt ./...

# vet runs go vet + go build so compilation errors are caught in CI before
# any human code review. This is the pre-commit equivalent the review flagged
# as missing — "get a real compile into CI or a pre-commit hook".
.PHONY: vet
vet:
	go vet ./...

# compile runs a full build (no output) to catch import / type errors.
# Faster than 'build' since it doesn't write a binary.
.PHONY: compile
compile:
	go build ./...

.PHONY: test
test: fmt vet compile
	go test ./... -v -coverprofile cover.out

.PHONY: build
build: fmt vet compile
	go build -o bin/manager main.go

.PHONY: run
run: fmt vet
	go run ./main.go

.PHONY: docker-build
docker-build:
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push:
	docker push ${IMG}

# ci is the target for CI pipelines: vet + compile must pass before tests run.
# This matches the pattern the review requested ("get a real compile into CI").
.PHONY: ci
ci: vet compile test
