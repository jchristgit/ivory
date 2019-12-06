all: check test

check: formatcheck typecheck lint

formatcheck:
	black --check $(BLACKARGS) ibory tests

typecheck:
	mypy --pretty $(MYPYARGS) ivory tests

lint:
	flake8 --show-source --max-line-length 99 $(FLAKEARGS) ivory tests

test:
	pytest --cov=ivory --cov-branch --doctest-modules $(PYTESTARGS) ivory tests

format:
	black $(BLACKARGS) ivory tests
