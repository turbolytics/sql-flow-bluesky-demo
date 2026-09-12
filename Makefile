# The pinned sqlflow image. Must match the Dockerfile and CI.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:v1.2.0

.PHONY: validate migrate psql run image clean

## validate: check pipeline.yml against the pinned image's config schema
validate:
	docker run --rm -v $(CURDIR)/pipeline.yml:/app/pipeline.yml \
		$(SQLFLOW_IMAGE) validate /app/pipeline.yml

## migrate: apply migrations to the compose database without starting the pipeline
migrate:
	docker compose up -d --wait postgres
	docker compose run --rm -T --no-deps --entrypoint /app/bin/migrate.sh sqlflow

## run: start postgres and the pipeline, following logs
run:
	docker compose up --build

## psql: open a shell on the compose database
psql:
	docker compose exec postgres psql -U bluesky -d bluesky

## image: build the worker image
image:
	docker build -t sqlflow-bluesky-demo .

## clean: stop compose and delete its volumes
clean:
	docker compose down -v
