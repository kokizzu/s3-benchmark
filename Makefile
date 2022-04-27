
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o s3-benchmark.ubuntu s3-benchmark.go
