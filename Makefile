build:
	@go build -o bin/fs

run: build
	@./bin/fs

test:
	@go test -v ./... -tags=unit

integration-test:
	@go test -v ./... -tags=integration