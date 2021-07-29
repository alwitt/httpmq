all: lint

.PHONY: lint
lint: .prepare ## Lint the files
	@go mod tidy
	@golangci-lint run ./...

.prepare: ## Prepare the project for local development
	@pip3 install --user pre-commit
	@pre-commit install
	@pre-commit install-hooks
	@GO111MODULE=on go get -v -u github.com/go-critic/go-critic/cmd/gocritic@v0.5.4
	@GO111MODULE=on go get -v -u github.com/swaggo/swag/cmd/swag
	@touch .prepare

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
