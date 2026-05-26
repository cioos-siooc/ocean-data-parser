lint:
	uv run ruff format .
	uv run ruff check --fix --select I .
	uv run ruff check --fix .

install-hooks:
	uv sync
	uv run pre-commit install
