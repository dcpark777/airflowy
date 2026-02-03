# Airflowy - Makefile
# This Makefile provides convenient commands to manage the Airflow Podman Compose setup

.PHONY: help run stop clean restart logs status init shell test check-resources fix-resources test-unit test-dags validate-dags check-naming check-resources install-precommit

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Airflowy - Available Commands:$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""

init: ## Initialize Airflow database and create admin user
	@echo "$(YELLOW)Initializing Airflow...$(NC)"
	podman-compose up airflow-init
	@echo "$(GREEN)Airflow initialization complete!$(NC)"

run: ## Build and launch Airflow containers in detached mode
	@echo "$(YELLOW)Building and starting Airflow containers...$(NC)"
	podman-compose up --build -d
	@echo "$(GREEN)Airflow is starting up!$(NC)"
	@echo "$(BLUE)Access the web UI at: http://localhost:8080$(NC)"
	@echo "$(BLUE)Default credentials: airflow / airflow$(NC)"

stop: ## Stop running containers without removing them
	@echo "$(YELLOW)Stopping Airflow containers...$(NC)"
	podman-compose stop
	@echo "$(GREEN)Containers stopped.$(NC)"

restart: ## Restart all containers
	@echo "$(YELLOW)Restarting Airflow containers...$(NC)"
	podman-compose restart
	@echo "$(GREEN)Containers restarted.$(NC)"

clean: ## Stop and remove containers, networks, and volumes
	@echo "$(YELLOW)Cleaning up Airflow containers and resources...$(NC)"
	@podman-compose stop 2>/dev/null || true
	@podman pod stop pod_airflowy 2>/dev/null || true
	@podman-compose down --remove-orphans 2>/dev/null || true
	@echo "$(GREEN)Cleanup complete.$(NC)"

clean-all: ## Stop and remove containers, networks, volumes, and images
	@echo "$(YELLOW)Performing full cleanup (containers, networks, volumes, and images)...$(NC)"
	@podman-compose stop 2>/dev/null || true
	@podman pod stop pod_airflowy 2>/dev/null || true
	@podman-compose down --volumes --rmi all --remove-orphans 2>/dev/null || true
	@echo "$(GREEN)Full cleanup complete.$(NC)"

logs: ## View logs from all containers (use LOGS_SERVICE=<service> for specific service)
	@if [ -z "$(LOGS_SERVICE)" ]; then \
		podman-compose logs -f; \
	else \
		podman-compose logs -f $(LOGS_SERVICE); \
	fi

status: ## Show status of all containers
	@echo "$(BLUE)Container Status:$(NC)"
	@podman-compose ps

shell: ## Open an interactive shell in the Airflow CLI container
	@echo "$(YELLOW)Opening Airflow CLI shell...$(NC)"
	podman-compose run --rm airflow-cli bash

test: ## Run all tests in container, then clean up test resources
	@echo "$(YELLOW)Running all tests in container...$(NC)"
	@EXIT_CODE=0; \
	podman-compose --profile test run --rm test || EXIT_CODE=$$?; \
	echo "$(YELLOW)Cleaning up test containers and resources...$(NC)"; \
	podman-compose --profile test down --volumes --remove-orphans 2>/dev/null || true; \
	if [ $$EXIT_CODE -ne 0 ]; then exit $$EXIT_CODE; fi
	@echo "$(GREEN)All tests complete. Test containers cleaned up.$(NC)"

test-local: ## Run all tests locally (requires pytest installed)
	@echo "$(YELLOW)Running all tests locally...$(NC)"
	pytest tests/ -v
	@echo "$(GREEN)All tests complete.$(NC)"

test-unit: ## Run unit tests for waiter plugin in container
	@echo "$(YELLOW)Running unit tests in container...$(NC)"
	podman-compose --profile test run --rm test pytest tests/test_waiter_*.py -v
	@echo "$(GREEN)Unit tests complete.$(NC)"

test-dags: ## Test DAG imports and structure in container
	@echo "$(YELLOW)Testing DAG imports in container...$(NC)"
	podman-compose --profile test run --rm test pytest tests/test_dag_imports.py -v
	@echo "$(GREEN)DAG import tests complete.$(NC)"

test-dag-best-practices: ## Test DAG best practices in container
	@echo "$(YELLOW)Testing DAG best practices in container...$(NC)"
	podman-compose --profile test run --rm test pytest tests/test_dag_best_practices.py -v
	@echo "$(GREEN)DAG best practices tests complete.$(NC)"

test-build: ## Build the test container image
	@echo "$(YELLOW)Building test container...$(NC)"
	podman-compose --profile test build test
	@echo "$(GREEN)Test container built.$(NC)"

test-shell: ## Open a shell in the test container
	@echo "$(YELLOW)Opening test container shell...$(NC)"
	podman-compose --profile test run --rm test bash

test-airflow: ## Run Airflow CLI tests (list DAGs)
	@echo "$(YELLOW)Running Airflow CLI tests...$(NC)"
	podman-compose run --rm airflow-cli airflow dags list
	@echo "$(GREEN)Airflow CLI tests complete.$(NC)"

check-resources: ## Check Podman machine resources
	@echo "$(BLUE)Podman machine resources:$(NC)"
	@podman machine list
	@echo "$(YELLOW)Note: Airflow requires at least 4GB RAM. Run 'make fix-resources' for help.$(NC)"

fix-resources: ## Show instructions to increase Podman machine memory
	@echo "$(BLUE)Increase Podman machine memory:$(NC)"
	@echo "  podman machine stop"
	@echo "  podman machine set --memory 4096"
	@echo "  podman machine start"

validate-dags: ## Validate DAG syntax and imports
	@echo "$(YELLOW)Validating DAGs...$(NC)"
	@python scripts/validate_dags.py
	@echo "$(GREEN)DAG validation complete.$(NC)"

check-naming: ## Check DAG naming conventions
	@echo "$(YELLOW)Checking DAG naming conventions...$(NC)"
	@python scripts/check_dag_naming.py
	@echo "$(GREEN)Naming check complete.$(NC)"

check-resources: ## Check DAG resource limits
	@echo "$(YELLOW)Checking DAG resource limits...$(NC)"
	@python scripts/check_dag_resources.py
	@echo "$(GREEN)Resource check complete.$(NC)"

install-precommit: ## Install pre-commit hooks
	@echo "$(YELLOW)Installing pre-commit hooks...$(NC)"
	@pip install pre-commit || echo "$(YELLOW)Warning: pip install pre-commit failed. Install manually: pip install pre-commit$(NC)"
	@pre-commit install || echo "$(YELLOW)Warning: pre-commit install failed$(NC)"
	@echo "$(GREEN)Pre-commit hooks installed.$(NC)"

