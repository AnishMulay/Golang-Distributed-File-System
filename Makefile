build:
	@go build -o bin/fs

run: build
	@./bin/fs

test:
	@go test -v ./...

build-daemon:
	@go build -o bin/fs-daemon ./daemon
	@chmod +x bin/fs-daemon

daemon: build-daemon
	@./bin/fs-daemon