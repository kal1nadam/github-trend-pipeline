SHELL := /bin/bash

include .env*
export

# Default to yesterday's date in UTC if DATE is not provided
DATE ?= $(shell date -u -d "yesterday" +%Y-%m-%d)

setup:
	python -m pipeline.setup
# 	python -m pipeline.transform

extract:
	python -m pipeline.extract --date $(DATE)

transform:
	python -m pipeline.transform

compute:
	python -m pipeline.compute --date $(DATE)

serve:
	uvicorn pipeline.serve:app --reload --port 8000

smoke:
	python -c "from pipeline.config import Settings; Settings.load(); print('Config OK')"

run: setup extract transform compute
	@echo "Pipeline run completed for date: $(DATE)"