PYTHON ?= python3
COMPOSE := docker compose --profile full

.PHONY: help install format lint typecheck test check compose-config image cluster-up node3-up cluster-down partition-node1 heal-node1 self-test

help:
	@echo "install         Install the package and development tools"
	@echo "format          Format Python sources"
	@echo "check           Run linting, type checks, and tests"
	@echo "compose-config  Validate the Docker Compose configuration"
	@echo "image           Build the Raft node image"
	@echo "cluster-up      Start the initially available node1 and node2"
	@echo "node3-up        Start the preconfigured third member"
	@echo "cluster-down    Stop the complete local cluster"
	@echo "partition-node1 Isolate node1 from Raft traffic"
	@echo "heal-node1      Restore node1's Raft network connection"
	@echo "self-test       Run the complete Docker acceptance scenario"

install:
	$(PYTHON) -m pip install -e ".[dev]"

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

lint:
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy

test:
	$(PYTHON) -m coverage run -m pytest
	$(PYTHON) -m coverage report

check: lint typecheck test compose-config

compose-config:
	$(COMPOSE) config --quiet

image:
	$(COMPOSE) build

cluster-up:
	docker compose up --build --detach node1 node2

node3-up:
	$(COMPOSE) up --build --detach node3

cluster-down:
	$(COMPOSE) down

partition-node1:
	sh ./scripts/partition-node.sh node1

heal-node1:
	sh ./scripts/heal-node.sh node1

self-test:
	$(PYTHON) ./scripts/run_acceptance_tests.py
