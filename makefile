
COMPOSE_FILE=airflow.yaml
ENV_FILE=.env

help: ## Shows this help message
	@echo "🛠️  Airflow Commands 🛠️"
	@echo "---------------------------------------------"
	@echo "make up      -> Start the whole system (detached mode)"
	@echo "make down    -> Stop and remove containers/networks"
	@echo "make stop    -> Stop containers only (without removing)"
	@echo "make restart -> Clean restart (Down + Up)"
	@echo "make logs    -> Follow logs in real-time"
	@echo "make shell   -> Open a bash shell inside the Scheduler"
	@echo "make check   -> Check for DAG import errors"

up: ## Start Airflow
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d --build
	@echo "🚀 Airflow is starting at http://localhost:8080"

down: ## Shut down and clean up
	docker compose -f $(COMPOSE_FILE) down
	@echo "👋 Airflow stopped and containers removed."

restart: down up ## Restart everything

gcr-login:
	@echo "Logging in to GitHub Container Registry..."
	@set -a && . ./.env && set +a && echo "$${CR_PAT}" | docker login ghcr.io -u thebluetonguegiraffe --password-stdin