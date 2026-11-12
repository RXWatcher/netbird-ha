# Commands assume the repository root is the cwd.
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
# The version NetBird sees is the upstream release this build is based on, with our own version as
# build metadata (ignored in comparisons): the dashboard's "update available" and client checks then
# compare against upstream releases, not against our pilot tag.
UPSTREAM_VERSION := $(shell awk '$$1=="github.com/netbirdio/netbird"{sub(/^v/,"",$$2);print $$2;exit}' go.mod)
HA_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
VERSION ?= $(UPSTREAM_VERSION)+netbird-ha.$(HA_VERSION)
LDFLAGS := -s -w -X github.com/netbirdio/netbird/version.version=$(VERSION)
UPSTREAM_DIR = $(shell go list -m -f '{{.Dir}}' github.com/netbirdio/netbird)

.PHONY: build test test-race test-dispatcher-integration lint check-upstream-callers upstream-mgmt-test images

build:
	CGO_ENABLED=1 go build -ldflags '$(LDFLAGS)' -o bin/netbird-mgmt ./cmd/management
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/netbird-signal ./cmd/signal

# dispatcher/ is its own module (it replaces upstream's signal-dispatcher), so ./... skips it.
test:
	go test ./...
	cd dispatcher && go test ./...

test-race:
	go test -race ./...
	cd dispatcher && go test -race ./...

# Valkey and Valkey+Sentinel containers on the host network; needs Docker.
test-dispatcher-integration:
	cd dispatcher && go test -tags integration -run Integration ./...

lint:
	$(GOLANGCI_LINT) run ./...
	cd dispatcher && $(GOLANGCI_LINT) run --build-tags integration ./...

check-upstream-callers:
	scripts/check-upstream-callers.sh

# Regression: upstream's management tests against the pinned upstream module (HA off).
# Runs from a writable copy of the module because the module cache is read-only.
upstream-mgmt-test:
	scripts/upstream-mgmt-test.sh $(PKGS)

images:
	docker build -f cmd/management/Dockerfile --build-arg VERSION=$(VERSION) -t netbird-ha/management:$(VERSION) .
	docker build -f cmd/signal/Dockerfile --build-arg VERSION=$(VERSION) -t netbird-ha/signal:$(VERSION) .

include deploy/bed.mk
