all: check test

check: formatcheck typecheck lint

formatcheck:
	black --check ivory tests

typecheck:
	mypy --pretty ivory tests

lint:
	flake8 --show-source --max-line-length 99 ivory tests

test:
	pytest --cov=ivory --cov-branch --doctest-modules $(PYTESTARGS) ivory tests

format:
	black ivory tests
