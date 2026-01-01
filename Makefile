SHELL := /bin/bash

include .env*
export

# Default to yesterday's date in UTC if DATE is not provided
DATE ?= $(shell date -u -d "yesterday" +%Y-%m-%d)

setup:
# 	python -m pipeline.transform --dry-run
	python -m pipeline.transform

extract:
	python -m pipeline.extract --date $(DATE)

transform:
	python -m pipeline.transform

compute:
	@echo "TODO: run python -m pipeline.compute --date $(DATE)"

run: setup extract transform compute