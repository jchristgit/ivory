all: formatcheck

formatcheck:
	black --check ivory

format:
	black ivory
