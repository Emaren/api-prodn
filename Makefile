# Makefile for AoE2HD Parsing App 🧠
PYTHONPATH := $(shell pwd)

# ─────────────────────────────
# 🔧 ENVIRONMENT
# ─────────────────────────────
ENV_FILE ?= .env

# ─────────────────────────────
# 🛠️ DEV TASKS (Native)
# ─────────────────────────────

dev: pg-start run

pg-start:
	@echo "🚀 Ensuring local PostgreSQL is running via Homebrew..."
	@brew services start postgresql@14 || true

run:
	@echo "🚀 Launching FastAPI locally with $(ENV_FILE)..."
	@ENV_FILE=$(ENV_FILE) ./run_local.sh

pg-shell:
	psql -U aoe2user -d aoe2db

pg-reset:
	@echo "♻️ Dropping + recreating local DB..."
	dropdb aoe2db --if-exists -U aoe2user
	createdb aoe2db -U aoe2user

pg-stop:
	@echo "🛑 Stopping local PostgreSQL..."
	@brew services stop postgresql@14

# ─────────────────────────────
# 🐳 DEV TASKS (Docker)
# ─────────────────────────────

dev-up:
	@echo "🟢 Starting Docker DB/PGAdmin only..."
	docker compose up db pgadmin -d

dev-down:
	@echo "🛑 Stopping Docker Dev Environment..."
	docker compose down -v

dev-reset:
	@echo "♻️ Resetting Docker Dev DB..."
	docker compose down -v && docker compose up db pgadmin -d

# ─────────────────────────────
# 🚀 PROD TASKS
# ─────────────────────────────

prod-up:
	docker compose -f docker-compose.prod.yml up -d --build

prod-down:
	docker compose -f docker-compose.prod.yml down -v

prod-rebuild:
	docker compose -f docker-compose.prod.yml down -v
	docker system prune -af --volumes
	docker compose -f docker-compose.prod.yml up -d --build

# ─────────────────────────────
# 🧬 MIGRATIONS
# ─────────────────────────────

new-migration:
	@read -p "Migration name: " name; \
	alembic revision --autogenerate -m "$$name"

migrate-dev:
	./scripts/migrate.sh local

migrate-prod:
	./scripts/migrate.sh render

stamp:
	alembic stamp head

# ─────────────────────────────
# 🔍 UTILITIES
# ─────────────────────────────

logs:
	docker compose logs -f

ps:
	docker compose ps

prune:
	docker system prune -af --volumes

# ─────────────────────────────
# 🎯 FULL STACK LAUNCH
# ─────────────────────────────

frontend:
	cd ../app-prodn && npm run dev

all:
	(ENV_FILE=$(ENV_FILE) ./run_local.sh &) && \
	cd ../app-prodn && npm run dev

frontend-tab:
	osascript -e 'tell app "Terminal" to do script "cd ~/projects/AoE2HDBets/app-prodn && npm run dev"'

backend-tab:
	osascript -e 'tell app "Terminal" to do script "cd ~/projects/AoE2HDBets/api-prodn && ENV_FILE=$(ENV_FILE) ./run_local.sh"'

# ─────────────────────────────
# 🔖 GIT TAG HELPER
# ─────────────────────────────

tag:
	git tag -a v$(tag) -m "$(msg)"
	git push origin v$(tag)

deploy-prod:
	@./deploy_to_prod.sh
