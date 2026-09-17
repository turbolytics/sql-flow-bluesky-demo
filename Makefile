# The pinned sqlflow image. Must match the Dockerfile and CI.
SQLFLOW_IMAGE ?= turbolytics/sql-flow:v2026.09.17.1

.PHONY: validate rollups migrate psql run serve image clean

## validate: check the configs against the pinned image's schemas, and check
## the generated rollup files still match rollups.yml
validate:
	docker run --rm -v $(CURDIR)/pipeline.yml:/app/pipeline.yml \
		$(SQLFLOW_IMAGE) validate /app/pipeline.yml
	docker run --rm -v $(CURDIR)/serve.yml:/app/serve.yml \
		$(SQLFLOW_IMAGE) validate /app/serve.yml
	docker run --rm -v $(CURDIR)/rollups.yml:/app/rollups.yml \
		$(SQLFLOW_IMAGE) validate /app/rollups.yml
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) rollup check \
		-c rollups.yml --migration migrations/0005_rollups.sql --serve serve.yml

## rollups: regenerate the migration from rollups.yml and print the dataset to
## paste into serve.yml. `make validate` fails until both match the file.
rollups:
	docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) rollup ddl \
		-c rollups.yml > migrations/0005_rollups.sql
	@echo "# regenerated migrations/0005_rollups.sql"
	@echo "# the posts_by_lang dataset for serve.yml, indented by four spaces:"
	@docker run --rm -v $(CURDIR):/w -w /w $(SQLFLOW_IMAGE) rollup serve \
		-c rollups.yml | sed 's/^/    /'

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
