# Contributing to pynenc

Thank you for your interest in contributing to pynenc!

**pynenc** is a distributed task orchestration library for Python. It solves hard problems in distributed systems — dependency-aware task scheduling, deadlock prevention through cycle detection, concurrency control across workers, and automatic recovery from runner failures — using a clean declarative API.

Contributions are welcome across all parts of the project:

- **pynenc** — core library: [github.com/pynenc/pynenc](https://github.com/pynenc/pynenc)
- **pynenc.org** — project website: [github.com/pynenc/pynenc.org](https://github.com/pynenc/pynenc.org)
- **docs** — Sphinx documentation, living inside the main repo under `docs/`

## Setting Up the Development Environment

1. **Fork and clone**:

   ```bash
   git clone https://github.com/YOUR_USERNAME/pynenc.git
   cd pynenc
   ```

2. **Install [UV](https://docs.astral.sh/uv/getting-started/installation/)** — pynenc uses UV for dependency management.

3. **Install all dependencies** (library, monitor, tests, docs):

   ```bash
   make install
   ```

4. **Install pre-commit hooks**:

   ```bash
   make install-pre-commit
   ```

5. **Install Docker** _(optional)_ — required for integration tests with Redis, MongoDB, or RabbitMQ backends. See [docs.docker.com/get-docker](https://docs.docker.com/get-docker/).

## Running Tests

```bash
# All tests with combined coverage
make test

# Unit tests only
make test-unit

# Integration tests only
make test-integration

# Coverage report in the console
make coverage

# HTML coverage report (opens at htmlcov/index.html)
make htmlcov
```

## Building Documentation

The docs live in `docs/` and are built with Sphinx + MyST.

```bash
# Check all docs dependencies are installed
make docs-check-deps

# Build HTML docs (output: docs/_build/html/index.html)
make docs

# Build and serve locally at http://localhost:8080
make docs-serve
```

Docstrings are written in **Markdown** (MyST syntax), not reStructuredText. See:

- [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html)
- [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html)

## Contributing to pynenc.org

The project website source is at [github.com/pynenc/pynenc.org](https://github.com/pynenc/pynenc.org). It is a separate repository — open a PR there for website content changes.

## Code Quality

The project uses pre-commit hooks for linting and formatting. Run them manually at any time:

```bash
make pre-commit
```

## Continuous Integration

GitHub Actions runs on every pull request:

1. Pre-commit checks
2. Unit tests
3. Integration tests
4. Combined coverage report

Make sure all checks pass before requesting a review.
