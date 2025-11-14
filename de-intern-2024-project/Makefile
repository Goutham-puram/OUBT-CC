.PHONY: help venv install install-dev test test-unit test-integration test-coverage lint format type-check clean deploy-infra destroy-infra download-data run-week1 run-week2 run-week3 run-week4

# Variables
PYTHON := python3
VENV := venv
VENV_BIN := $(VENV)/bin
PIP := $(VENV_BIN)/pip
PYTEST := $(VENV_BIN)/pytest
BLACK := $(VENV_BIN)/black
FLAKE8 := $(VENV_BIN)/flake8
MYPY := $(VENV_BIN)/mypy
TERRAFORM := terraform
TERRAFORM_DIR := infrastructure/terraform

# Default target
help:
	@echo "Data Engineering Internship 2024 - Available Commands"
	@echo "======================================================"
	@echo ""
	@echo "Environment Setup:"
	@echo "  make venv           - Create Python virtual environment"
	@echo "  make install        - Install production dependencies"
	@echo "  make install-dev    - Install development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make lint           - Run code linters (flake8, black check)"
	@echo "  make format         - Format code with black"
	@echo "  make type-check     - Run mypy type checking"
	@echo "  make clean          - Clean temporary files and caches"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make deploy-infra   - Deploy AWS infrastructure with Terraform"
	@echo "  make destroy-infra  - Destroy AWS infrastructure"
	@echo "  make tf-plan        - Show Terraform execution plan"
	@echo "  make tf-init        - Initialize Terraform"
	@echo ""
	@echo "Data Operations:"
	@echo "  make download-data  - Download NYC Yellow Taxi sample data"
	@echo "  make run-week1      - Execute Week 1 scripts"
	@echo "  make run-week2      - Execute Week 2 scripts"
	@echo "  make run-week3      - Execute Week 3 scripts"
	@echo "  make run-week4      - Execute Week 4 scripts"
	@echo ""

# Environment setup
venv:
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created. Activate with: source $(VENV_BIN)/activate"

install: venv
	@echo "Installing production dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .
	@echo "Production dependencies installed successfully!"

install-dev: install
	@echo "Installing development dependencies..."
	$(PIP) install -r requirements-dev.txt
	@echo "Development dependencies installed successfully!"

# Testing
test:
	@echo "Running all tests..."
	$(PYTEST) tests/ -v

test-unit:
	@echo "Running unit tests..."
	$(PYTEST) tests/unit/ -v

test-integration:
	@echo "Running integration tests..."
	$(PYTEST) tests/integration/ -v

test-coverage:
	@echo "Running tests with coverage..."
	$(PYTEST) tests/ --cov=src/de_intern_2024 --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/index.html"

# Code Quality
lint:
	@echo "Running flake8..."
	$(FLAKE8) src/ tests/
	@echo "Running black check..."
	$(BLACK) --check src/ tests/
	@echo "Linting completed!"

format:
	@echo "Formatting code with black..."
	$(BLACK) src/ tests/
	@echo "Code formatted successfully!"

type-check:
	@echo "Running mypy type checking..."
	$(MYPY) src/
	@echo "Type checking completed!"

# Infrastructure
tf-init:
	@echo "Initializing Terraform..."
	cd $(TERRAFORM_DIR) && $(TERRAFORM) init

tf-plan: tf-init
	@echo "Creating Terraform plan..."
	cd $(TERRAFORM_DIR) && $(TERRAFORM) plan

deploy-infra: tf-init
	@echo "Deploying infrastructure..."
	cd $(TERRAFORM_DIR) && $(TERRAFORM) apply -auto-approve
	@echo "Infrastructure deployed successfully!"

destroy-infra:
	@echo "Destroying infrastructure..."
	@read -p "Are you sure you want to destroy all infrastructure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		cd $(TERRAFORM_DIR) && $(TERRAFORM) destroy; \
	fi

# Data Operations
download-data:
	@echo "Downloading NYC Yellow Taxi sample data..."
	$(PYTHON) data/scripts/download_taxi_data.py
	@echo "Data downloaded successfully!"

run-week1:
	@echo "Running Week 1 scripts..."
	$(PYTHON) -m de_intern_2024.week1.main
	@echo "Week 1 completed!"

run-week2:
	@echo "Running Week 2 scripts..."
	$(PYTHON) -m de_intern_2024.week2.main
	@echo "Week 2 completed!"

run-week3:
	@echo "Running Week 3 scripts..."
	$(PYTHON) -m de_intern_2024.week3.main
	@echo "Week 3 completed!"

run-week4:
	@echo "Running Week 4 scripts..."
	$(PYTHON) -m de_intern_2024.week4.main
	@echo "Week 4 completed!"

# Cleanup
clean:
	@echo "Cleaning up temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/
	@echo "Cleanup completed!"
