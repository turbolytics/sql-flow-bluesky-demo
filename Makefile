# The pinned sqlflow image. Must match the Dockerfile and CI.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:v2026.09.14.1

.PHONY: validate migrate psql run serve image clean

## validate: check pipeline.yml and serve.yml against the pinned image's schemas
validate:
	docker run --rm -v $(CURDIR)/pipeline.yml:/app/pipeline.yml \
		$(SQLFLOW_IMAGE) validate /app/pipeline.yml
	docker run --rm -v $(CURDIR)/serve.yml:/app/serve.yml \
		$(SQLFLOW_IMAGE) validate /app/serve.yml

## migrate: apply migrations to the compose database without starting the pipeline
migrate:
	docker compose up -d --wait postgres
	docker compose run --rm -T --no-deps --entrypoint /app/bin/migrate.sh sqlflow

## run: start postgres and the pipeline, following logs
run:
	docker compose up --build

## serve: start postgres and the API, following logs
serve:
	docker compose up --build postgres api

## psql: open a shell on the compose database
psql:
	docker compose exec postgres psql -U bluesky -d bluesky

## image: build the image the worker and the API share
image:
	docker build --build-arg SQLFLOW_IMAGE=$(SQLFLOW_IMAGE) -t sqlflow-bluesky-demo .

## clean: stop compose and delete its volumes
clean:
	docker compose down -v
