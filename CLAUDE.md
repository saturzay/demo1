- Python 3.13, managed with uv

- Install: uv sync
- Test: uv run pytest tests/ -x
- Single test: uv run pytest tests/test_foo.py::test_bar
- Lint/format: uv run ruff check --fix && uv run ruff format
- Type check: uv run mypy src/