VENV := .venv
PY   := $(VENV)/bin/python
export PYTHONPATH := src

.PHONY: setup seed load change inspect demo test clean

setup:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements.txt
	@echo "ready -- run 'make demo'"

seed:
	$(PY) -m warehouse.cli seed

load:
	$(PY) -m warehouse.cli load

change:
	$(PY) -m warehouse.cli change

inspect:
	$(PY) -m warehouse.cli inspect

demo: clean-db seed load change load inspect

clean-db:
	@rm -f data/warehouse.duckdb data/warehouse.duckdb.wal

test:
	PYTHONPATH=src:tests $(VENV)/bin/pytest tests -q

clean: clean-db
	rm -rf .pytest_cache
	find . -name __pycache__ -type d -exec rm -rf {} +
