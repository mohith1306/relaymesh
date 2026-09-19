.PHONY: build build-go build-ai test test-go test-ai lint clean help

GO := go
GOFLAGS := -v
BINARY_DIR := bin
PYTHON := python3
VENV := .venv
PROTO_DIR := api/proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: build-go ## Build Go binaries

build-go: ## Build Go binaries
	$(GO) build $(GOFLAGS) -o $(BINARY_DIR)/relaymesh ./cmd/relaymesh
	$(GO) build $(GOFLAGS) -o $(BINARY_DIR)/gateway ./cmd/gateway

build-ai: ## Build Python AI service
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install grpcio grpcio-tools protobuf numpy

test: test-go ## Run Go tests

test-go: ## Run Go tests
	$(GO) test -v ./tests/...

lint: ## Run Go linter
	$(GO) vet ./...

clean: ## Clean build artifacts
	rm -rf $(BINARY_DIR) $(VENV)

run-node: build-go ## Run a relaymesh node (usage: make run-node NODE_ID=node-a PORT=9001)
	./$(BINARY_DIR)/relaymesh --node-id $(NODE_ID) --port $(PORT) --log-level debug

run-gateway: build-go ## Run gateway node
	./$(BINARY_DIR)/gateway
