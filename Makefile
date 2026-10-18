.PHONY: install lint test demo bundle-validate deploy-dev tf-plan-dev

PY ?= python

install:
	$(PY) -m pip install -e ".[dev]"

lint:
	ruff check src tests pipelines
	ruff format --check src tests pipelines

test:
	pytest --cov=trade_intel --cov-report=term-missing

# Corre el pipeline completo en local (Spark + Parquet) con los datos de muestra
demo:
	$(PY) scripts/run_local_demo.py

bundle-validate:
	databricks bundle validate -t dev

deploy-dev:
	databricks bundle deploy -t dev

tf-plan-dev:
	cd infra/terraform && terraform init -backend-config=envs/dev.backend.hcl && terraform plan -var-file=envs/dev.tfvars
