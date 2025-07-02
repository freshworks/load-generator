SHELL := /bin/bash
BINARY = lg
BUILDFLAGS = $(LDFLAGS) $(EXTRAFLAGS)

.PHONY: $(BINARY)
$(BINARY): Makefile
	CGO_ENABLED=0 go build -o $(BINARY) $(BUILDFLAGS)

test:
	go test -count=1 -race ./...

.PHONY: lint
lint:
	golint ./...

.PHONY: clean
clean:
	rm -f $(BINARY) $(TARGETS)
	rm -rf ./dist

.PHONY: snapshot
snapshot: clean
	goreleaser --snapshot --skip-validate --skip-publish

.PHONY: release
release: clean
	goreleaser --skip-validate --skip-publish

.PHONY: publish
publish: clean
	goreleaser
