.PHONY: all clean test vaultic

all: vaultic

vaultic:
	go run build.go

clean:
	rm -f vaultic

test:
	go test ./cmd/... ./internal/...

