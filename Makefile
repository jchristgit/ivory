all: formatcheck lint

formatcheck:
	black --check ivory

lint:
	flake8 --show-source --max-line-length 99 ivory

format:
	black ivory
