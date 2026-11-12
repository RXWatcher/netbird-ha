# Commands assume the repository root is the cwd.
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/netbirdio/netbird/version.version=$(VERSION)
UPSTREAM_DIR = $(shell go list -m -f '{{.Dir}}' github.com/netbirdio/netbird)

.PHONY: build test test-race lint check-upstream-callers upstream-mgmt-test images

build:
	CGO_ENABLED=1 go build -ldflags '$(LDFLAGS)' -o bin/netbird-mgmt ./cmd/management
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/netbird-signal ./cmd/signal

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run ./...

check-upstream-callers:
	scripts/check-upstream-callers.sh

# Regression: upstream's management tests against the pinned upstream module (HA off).
# Runs from a writable copy of the module because the module cache is read-only.
upstream-mgmt-test:
	scripts/upstream-mgmt-test.sh $(PKGS)

images:
	docker build -f cmd/management/Dockerfile --build-arg VERSION=$(VERSION) -t netbird-ha/management:$(VERSION) .
	docker build -f cmd/signal/Dockerfile --build-arg VERSION=$(VERSION) -t netbird-ha/signal:$(VERSION) .
