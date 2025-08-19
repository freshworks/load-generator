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

# Automated testing tasks for each endpoint

.PHONY: test-kafka-scram
test-kafka-scram: $(BINARY)
	docker compose up -d zookeeper kafka-scram
	sleep 15
	./$(BINARY) kafka --brokers "localhost:9093" --topic "test-scram-topic" --message "Test message" \
		--username "admin" --password "admin-secret" --sasl-mechanism "SCRAM-SHA-512" \
		--requestrate 10 --duration 5s
	docker compose down

.PHONY: test-kafka
test-kafka: $(BINARY)
	docker compose up -d zookeeper kafka
	sleep 15
	./$(BINARY) kafka --brokers "localhost:9092" --topic "test-topic" --message "Test message" \
		--requestrate 10 --duration 5s
	docker compose down

.PHONY: test-redis
test-redis: $(BINARY)
	docker compose up -d redis
	sleep 5
	./$(BINARY) redis --cmd "SET" --arg "test-key" --arg "Test value" \
		--requestrate 10 --duration 5s localhost:6379
	docker compose down

.PHONY: test-http
test-http: $(BINARY)
	./$(BINARY) http --requestrate 10 --duration 5s \
		"https://httpbin.org/get"

.PHONY: test-mongo
test-mongo: $(BINARY)
	docker compose up -d mongodb
	sleep 10
	./$(BINARY) mongo --database testdb --collection users --operation find \
		--filter '{"status":"active"}' --requestrate 10 --duration 5s \
		"mongodb://admin:admin@localhost:27017"
	docker compose down

.PHONY: test-all
test-all: test-kafka-scram test-kafka test-redis test-http test-mongo
	docker compose down
