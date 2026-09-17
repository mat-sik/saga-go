.PHONY: test
test:
	go test ./...

VERSION ?= $(shell git tag -l 'saga/v*' | sed 's#saga/v##' | sort -V | tail -1)

.PHONY: docker-build-mail-sender docker-build-tx-consumer docker-build-tx-producer docker-build-tx-validator docker-build

docker-build-mail-sender:
	docker build -f cmd/mail-sender/Dockerfile -t mail-sender:$(VERSION) .

docker-build-tx-consumer:
	docker build -f cmd/tx-consumer/Dockerfile -t tx-consumer:$(VERSION) .

docker-build-tx-producer:
	docker build -f cmd/tx-producer/Dockerfile -t tx-producer:$(VERSION) .

docker-build-tx-validator:
	docker build -f cmd/tx-validator/Dockerfile -t tx-validator:$(VERSION) .

docker-build: docker-build-mail-sender docker-build-tx-consumer docker-build-tx-producer docker-build-tx-validator
