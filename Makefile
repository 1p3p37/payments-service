# Совместим с GNU make 3.81+ (штатный make на macOS) и с любым /bin/sh:
# на Debian/Ubuntu это dash, на macOS — bash 3.2. Рецепты — POSIX sh, без bash-измов.

EXTRA ?=
CONSUMERS ?= 3

.DEFAULT_GOAL := help

# ── Определение среды ────────────────────────────────────────────────────────

OS := $(shell uname -s 2>/dev/null || echo unknown)

# bash, если есть, иначе POSIX sh — рецепты работают в обоих
BASH_PATH := $(shell command -v bash 2>/dev/null)
ifneq ($(BASH_PATH),)
SHELL := $(BASH_PATH)
else
SHELL := /bin/sh
endif

# Python-окружение: uv (рекомендуется) → локальный .venv → ошибка с подсказкой
UV := $(shell command -v uv 2>/dev/null)
ifneq ($(UV),)
PY := uv run python
PY_ENV := uv ($(UV))
else ifneq ($(wildcard .venv/bin/python),)
PY := .venv/bin/python
PY_ENV := .venv без uv
else
PY := @echo "Не найдены ни uv, ни .venv. Установите uv: https://docs.astral.sh/uv/getting-started/installation/" >&2; exit 1;
PY_ENV := не найдено
endif

# Docker Compose: плагин v2 (`docker compose`) → отдельный бинарь v1 (`docker-compose`)
COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || { command -v docker-compose >/dev/null 2>&1 && echo docker-compose; })
ifeq ($(COMPOSE),docker compose)
COMPOSE_WAIT := --wait
else
COMPOSE_WAIT :=
endif
ifeq ($(COMPOSE),)
COMPOSE := @echo "Docker Compose не найден: https://docs.docker.com/compose/install/" >&2; exit 1;
endif

# ── Справка ──────────────────────────────────────────────────────────────────

.PHONY: help
help:  ## Показать список команд
	@echo "Payments Service Makefile"
	@echo "========================="
	@grep -E '^[a-zA-Z_0-9%-]+:.*?## .*$$' $(word 1,$(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env:  ## Показать определённую среду (ОС, shell, Python, Docker Compose)
	@echo "OS:              $(OS)"
	@echo "make shell:      $(SHELL)"
	@echo "Python:          $(PY_ENV)"
	@echo "Docker Compose:  $(COMPOSE)"
	@if docker info >/dev/null 2>&1; then echo "Docker daemon:   доступен"; else echo "Docker daemon:   недоступен (integration и e2e тесты будут пропущены)"; fi

# ── Разработка ───────────────────────────────────────────────────────────────

.PHONY: install
install:  ## Установить зависимости (нужен uv)
ifeq ($(UV),)
	$(error uv не найден. Установка: curl -LsSf https://astral.sh/uv/install.sh | sh)
endif
	@echo "Installing dependencies..."
	@uv sync
	@echo "Dependencies installed"

.PHONY: lint
lint:  ## Проверить код: ruff check + ruff format --check
	@$(PY) -m ruff check .
	@$(PY) -m ruff format --check .

.PHONY: format
format:  ## Отформатировать код и применить автоисправления ruff
	@$(PY) -m ruff format .
	@$(PY) -m ruff check --fix .

.PHONY: typecheck
typecheck:  ## Проверить типы: mypy strict
	@$(PY) -m mypy

.PHONY: check
check: lint typecheck test  ## Всё перед коммитом: lint + typecheck + test

# ── Тесты ────────────────────────────────────────────────────────────────────

.PHONY: test
test:  ## Unit + integration (Docker); e2e пропускаются без стека. Аргументы: EXTRA="-k consumer"
	@echo "Starting tests $(EXTRA)..."
	@$(PY) -m pytest $(EXTRA)

.PHONY: test-unit
test-unit:  ## Только unit-тесты (Docker не нужен)
	@$(PY) -m pytest tests/unit $(EXTRA)

.PHONY: test-integration
test-integration:  ## Только integration-тесты (testcontainers, нужен Docker)
	@$(PY) -m pytest tests/integration $(EXTRA)

.PHONY: test-e2e
test-e2e:  ## e2e на поднятом стеке (сначала make up)
	@$(PY) -m pytest -m e2e tests/e2e $(EXTRA)

# ── Docker Compose ───────────────────────────────────────────────────────────

.PHONY: up
up:  ## Собрать и запустить стек
	@echo "Starting stack..."
	@$(COMPOSE) up --build -d $(COMPOSE_WAIT)
	@$(COMPOSE) ps

.PHONY: down
down:  ## Остановить стек (данные сохраняются)
	@$(COMPOSE) down

.PHONY: clean
clean:  ## Остановить стек и удалить данные Postgres и RabbitMQ
	@$(COMPOSE) down -v

.PHONY: ps
ps:  ## Состояние сервисов
	@$(COMPOSE) ps -a

.PHONY: logs
logs:  ## Логи сервисов: make logs EXTRA="consumer"
	@$(COMPOSE) logs -f --tail 100 $(EXTRA)

.PHONY: scale
scale:  ## Запустить несколько consumer'ов: make scale CONSUMERS=3
	@$(COMPOSE) up -d --scale consumer=$(CONSUMERS)

# ── Миграции ─────────────────────────────────────────────────────────────────

.PHONY: migrate
migrate:  ## Применить миграции (сервис migrate в compose)
	@echo "Applying migrations..."
	@$(COMPOSE) run --rm migrate
	@echo "Migrations applied"

.PHONY: create-migration
create-migration:  ## Создать миграцию autogenerate: make create-migration EXTRA="add_index" REV=0003
ifndef EXTRA
	$(error EXTRA variable is not set. Usage: make create-migration EXTRA="migration_name" [REV=0003])
endif
	@echo "Creating migration $(EXTRA)..."
	@# генерируется в контейнере против БД стека; каталог migrations смонтирован с хоста.
	@# REV — номер ревизии в стиле проекта (0001, 0002, …); без него alembic даст хэш
	@$(COMPOSE) run --rm -v "$(CURDIR)/migrations:/app/migrations" migrate alembic revision --autogenerate -m "$(EXTRA)" $(if $(REV),--rev-id $(REV),)
	@echo "Migration created in migrations/versions/ — проверьте и поправьте вручную"
