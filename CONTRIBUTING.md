# Contributing to aUniqORM

Thank you for contributing to aUniqORM.

## Development Setup

Use Python 3.12 or newer. Install development dependencies with uv:

```bash
uv sync --extra dev
```

Optionally install the repository's pre-commit hooks to run Ruff before each
commit:

```bash
uv run pre-commit install
```

Run focused tests while developing, then run the full suite and formatting
checks before opening a pull request:

```bash
uv run pytest tests/test_models.py
uv run pytest
uvx ruff check --fix
uvx ruff format
cargo check --manifest-path rust/Cargo.toml
```

## Pull Requests

Keep pull requests focused and include tests for changed behavior. Preserve
support for Python 3.12 through 3.14. Document public API, migration, or
database-behavior changes in the README when they affect users.

Do not include credentials, private endpoints, customer data, or other
confidential information in issues, commits, tests, or pull requests. Report
security issues privately as described in [SECURITY.md](SECURITY.md).

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).