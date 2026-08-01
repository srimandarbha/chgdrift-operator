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

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: fmt vet
	go test ./... -coverprofile cover.out

.PHONY: build
build: fmt vet
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
