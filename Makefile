SHELL := /bin/bash

include .env
export

# Default to today's date in UTC if DATE is not provided
DATE ?= $(shell date -u +%Y-%m-%d)

setup:
	@echo "TODO: run setup SQL (datasets + tables) via Python runner"

extract:
	@echo "TODO: run python -m pipeline.extract --date $(DATE)"

transform:
	@echo "TODO: run python -m pipeline.transform --date $(DATE)"

compute:
	@echo "TODO: run python -m pipeline.compute --date $(DATE)"

run: extract transform compute