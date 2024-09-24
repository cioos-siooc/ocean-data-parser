lint:
	ruff format .
	ruff check --fix --select I .
	ruff check --fix .