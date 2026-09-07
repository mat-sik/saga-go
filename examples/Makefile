.PHONY: test
test:
	go test ./...

VERSION ?= $(shell git describe --tags --abbrev=0 --match 'saga-go/v*' | sed 's#saga-go/v##')

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
