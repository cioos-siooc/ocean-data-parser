lint:
	uv run ruff format .
	uv run ruff check --fix --select I .
	uv run ruff check --fix .