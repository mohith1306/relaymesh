.PHONY: build test clean proto help

# Go variables
GO := go
GOFLAGS := -v
BINARY_DIR := bin

# Python variables
PYTHON := python3
VENV := .venv

# Proto variables
PROTO_DIR := api/proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: build-go build-ai ## Build all components

build-go: ## Build Go binaries
	$(GO) build $(GOFLAGS) -o $(BINARY_DIR)/relaymesh ./cmd/relaymesh
	$(GO) build $(GOFLAGS) -o $(BINARY_DIR)/gateway ./cmd/gateway

build-ai: $(VENV)/bin/activate ## Build Python AI service
	$(VENV)/bin/pip install -e .

$(VENV)/bin/activate: pyproject.toml
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -e .

proto: proto-go proto-python ## Generate all proto files

proto-go: ## Generate Go proto files
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	PATH=$$PATH:$(shell $(GO) env GOPATH)/bin protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

proto-python: $(VENV)/bin/activate ## Generate Python proto files
	$(VENV)/bin/python -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=ai/service/ \
		--grpc_python_out=ai/service/ \
		$(PROTO_FILES)

test: test-go test-ai ## Run all tests

test-go: ## Run Go tests
	$(GO) test -v ./...

test-ai: $(VENV)/bin/activate ## Run Python tests
	$(VENV)/bin/python -m pytest tests/ -v

lint: lint-go lint-ai ## Run all linters

lint-go: ## Run Go linter
	$(GO) vet ./...

lint-ai: $(VENV)/bin/activate ## Run Python linter
	$(VENV)/bin/python -m flake8 ai/ --max-line-length=100

clean: ## Clean build artifacts
	rm -rf $(BINARY_DIR) $(VENV) ai/service/relaymesh_pb2*.py ai/service/__pycache__

run-node: build-go ## Run a relaymesh node
	./$(BINARY_DIR)/relaymesh --node-id $(NODE_ID) --port $(PORT)

run-ai: build-ai ## Run AI service
	$(VENV)/bin/python -m ai.service.server

run-gateway: build-go ## Run gateway node
	./$(BINARY_DIR)/gateway
