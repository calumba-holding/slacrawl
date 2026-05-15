BINARY ?= bin/slacrawl
COMPLETION_DIR ?= dist/completions

.PHONY: build test fmt run generate-sqlc completion completion-bash completion-zsh clean

build:
	mkdir -p $(dir $(BINARY))
	go build -o $(BINARY) ./cmd/slacrawl

test:
	go test ./...

fmt:
	gofmt -w cmd internal

run:
	go run ./cmd/slacrawl $(ARGS)

generate-sqlc:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

completion: completion-bash completion-zsh

completion-bash:
	mkdir -p $(COMPLETION_DIR)
	go run ./cmd/slacrawl completion bash > $(COMPLETION_DIR)/slacrawl.bash

completion-zsh:
	mkdir -p $(COMPLETION_DIR)
	go run ./cmd/slacrawl completion zsh > $(COMPLETION_DIR)/_slacrawl

clean:
	rm -rf bin dist
