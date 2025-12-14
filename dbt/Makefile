.PHONY: deps seed run test docs docs-serve lint lint-fix clean all fresh debug compile unit-test

DBT_PROFILES_DIR := .
DBT_PROJECT_DIR := .

deps:
	@echo "Installing dbt dependencies..."
	dbt deps --profiles-dir $(DBT_PROFILES_DIR)

seed:
	@echo "Loading seeds..."
	dbt seed --profiles-dir $(DBT_PROFILES_DIR)

run:
	@echo "Running dbt models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR)

test:
	@echo "Running dbt tests..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR)

unit-test:
	@echo "Running unit tests..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select "test_type:unit"

compile:
	@echo "Compiling dbt models..."
	dbt compile --profiles-dir $(DBT_PROFILES_DIR)

debug:
	@echo "Running dbt debug..."
	dbt debug --profiles-dir $(DBT_PROFILES_DIR)

docs:
	@echo "Generating documentation..."
	dbt docs generate --profiles-dir $(DBT_PROFILES_DIR)

docs-serve: docs
	@echo "Serving documentation..."
	dbt docs serve --profiles-dir $(DBT_PROFILES_DIR)

lint:
	@echo "Linting SQL files..."
	sqlfluff lint models/ macros/ tests/

lint-fix:
	@echo "Fixing SQL files..."
	sqlfluff fix models/ macros/ tests/ --force

pre-commit-install:
	@echo "Installing pre-commit hooks..."
	pre-commit install

pre-commit-run:
	@echo "Running pre-commit on all files..."
	pre-commit run --all-files

clean:
	@echo "Cleaning dbt artifacts..."
	dbt clean --profiles-dir $(DBT_PROFILES_DIR)
	rm -rf logs/
	rm -rf target/

all: deps seed run test
	@echo "Full dbt pipeline completed"

	@echo "Fresh build completed"

	@echo "Running staging models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR) --select staging

run-marts:
	@echo "Running mart models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR) --select marts

test-staging:
	@echo "Testing staging models..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select staging

test-marts:
	@echo "Testing mart models..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select marts