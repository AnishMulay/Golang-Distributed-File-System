build:
	@go build -o bin/fs

run: build
	@./bin/fs

clean:
	@rm -rf bin/
	@find . -type d -name ':*' -exec rm -rf {} +

test:
	@go test -v ./... -tags=unit

integration-test:
	@go test -v ./... -tags=integration