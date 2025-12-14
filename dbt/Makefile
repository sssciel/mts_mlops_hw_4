.PHONY: deps seed run test docs docs-serve lint lint-fix clean all fresh debug compile unit-test

# Переменные
DBT_PROFILES_DIR := .
DBT_PROJECT_DIR := .

# Базовые команды dbt
deps:
	@echo "📦 Installing dbt dependencies..."
	dbt deps --profiles-dir $(DBT_PROFILES_DIR)

seed:
	@echo "🌱 Loading seeds..."
	dbt seed --profiles-dir $(DBT_PROFILES_DIR)

run:
	@echo "🚀 Running dbt models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR)

test:
	@echo "🧪 Running dbt tests..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR)

unit-test:
	@echo "🔬 Running unit tests..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select "test_type:unit"

compile:
	@echo "📝 Compiling dbt models..."
	dbt compile --profiles-dir $(DBT_PROFILES_DIR)

debug:
	@echo "🔍 Running dbt debug..."
	dbt debug --profiles-dir $(DBT_PROFILES_DIR)

# Документация
docs:
	@echo "📚 Generating documentation..."
	dbt docs generate --profiles-dir $(DBT_PROFILES_DIR)

docs-serve: docs
	@echo "🌐 Serving documentation..."
	dbt docs serve --profiles-dir $(DBT_PROFILES_DIR)

# SQLFluff линтинг
lint:
	@echo "🔎 Linting SQL files..."
	sqlfluff lint models/ macros/ tests/

lint-fix:
	@echo "🔧 Fixing SQL files..."
	sqlfluff fix models/ macros/ tests/ --force

# Pre-commit
pre-commit-install:
	@echo "🪝 Installing pre-commit hooks..."
	pre-commit install

pre-commit-run:
	@echo "▶️ Running pre-commit on all files..."
	pre-commit run --all-files

# Очистка
clean:
	@echo "🧹 Cleaning dbt artifacts..."
	dbt clean --profiles-dir $(DBT_PROFILES_DIR)
	rm -rf logs/
	rm -rf target/

# Полный цикл
all: deps seed run test
	@echo "✅ Full dbt pipeline completed!"

# Свежий запуск (очистка + полный цикл)
fresh: clean all
	@echo "🎉 Fresh build completed!"

# Запуск отдельных моделей
run-staging:
	@echo "🚀 Running staging models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR) --select staging

run-marts:
	@echo "🚀 Running mart models..."
	dbt run --profiles-dir $(DBT_PROFILES_DIR) --select marts

# Тестирование отдельных слоёв
test-staging:
	@echo "🧪 Testing staging models..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select staging

test-marts:
	@echo "🧪 Testing mart models..."
	dbt test --profiles-dir $(DBT_PROFILES_DIR) --select marts

# Справка
help:
	@echo "Available commands:"
	@echo "  make deps          - Install dbt dependencies"
	@echo "  make seed          - Load seed files"
	@echo "  make run           - Run all models"
	@echo "  make test          - Run all tests"
	@echo "  make unit-test     - Run unit tests only"
	@echo "  make compile       - Compile models"
	@echo "  make debug         - Debug connection"
	@echo "  make docs          - Generate documentation"
	@echo "  make docs-serve    - Generate and serve documentation"
	@echo "  make lint          - Lint SQL files"
	@echo "  make lint-fix      - Fix SQL files"
	@echo "  make clean         - Clean artifacts"
	@echo "  make all           - Run full pipeline"
	@echo "  make fresh         - Clean and run full pipeline"
	@echo "  make run-staging   - Run staging models only"
	@echo "  make run-marts     - Run mart models only"
	@echo "  make test-staging  - Test staging models only"
	@echo "  make test-marts    - Test mart models only"
	@echo "  make help          - Show this help"
