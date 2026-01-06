.PHONY: backend-dev backend-test backend-docker backend-docker-run


BE_DIR := container
IMAGE_NAME := kafka-chase

status-ports:
	@lsof -i tcp:8080
	@lsof -i tcp:4200


backend-dev: export CAPTURE_LOG=INFO
backend-dev:
	cd ${BE_DIR} && cargo watch  -x "run -- run --config test-data/config-localhost.yaml --secrets test-data/secrets"

backend-test:
	cd ${BE_DIR} && cargo watch --ignore test_data -x "test"

backend-docker: PKG_NAME=kafka-chase
backend-docker:
	{ \
	docker buildx build ${BE_DIR} -t $(IMAGE_NAME) -f ${BE_DIR}/Dockerfile; \
	docker image ls $(IMAGE_NAME); \
	}

backend-docker-run: backend-docker
	docker run -it --rm --name $(IMAGE_NAME)-backend -p 8080:8080 --mount type=bind,src=$(PWD)/${BE_DIR}/test-data,dst=/test-data  \
	-e CAPTURE_LOG=INFO \
	-e APP_PERSISTENCE__DB__CONNECTION__URL=postgres://host.docker.internal:5432/service-capture \
	$(IMAGE_NAME)-backend start --config /test-data/config-localhost.yaml --secrets /test-data/secrets
