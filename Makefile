.DEFAULT_GOAL := all
isort = isort -rc qcfractal
black = black qcfractal
autoflake = autoflake -ir --remove-all-unused-imports --ignore-init-module-imports --remove-unused-variables --exclude="*/tests/*,*/versions/*,*/models/*" qcfractal

.PHONY: install
install:
	pip install -e .

.PHONY: format
format:
#	$(autoflake)
#	$(isort)
	$(black)

.PHONY: lint
lint:
	$(isort) --check-only
	$(black) --check

.PHONY: check-dist
check-dist:
	python setup.py check -ms
	python setup.py sdist
	twine check dist/*

.PHONY: mypy
mypy:
	mypy qcfractal

.PHONY: test
test:
	pytest -v --cov=qcfractal/

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	rm -rf dist
	rm -f qcfractal/*.c qcfractal/*.so
	python setup.py clean
