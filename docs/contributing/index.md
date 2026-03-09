# Contributing to Pynenc

Set up your development environment and contribute to Pynenc.

**pynenc** is a distributed task orchestration library for Python. It solves hard problems in distributed systems — dependency-aware task scheduling, deadlock prevention, concurrency control, and automatic runner recovery — using a clean declarative API.

Contributions are welcome across:

- **pynenc** — core library (this repo)
- **pynenc.org** — project website at [github.com/pynenc/pynenc.org](https://github.com/pynenc/pynenc.org)
- **docs** — Sphinx documentation, living in the `docs/` directory of this repo

```{toctree}
:hidden:
:maxdepth: 2
:caption: Contributing

./docs
```

## Setting Up the Development Environment

1. **Fork and Clone**:

   ```bash
   git clone https://github.com/<your-username>/pynenc.git
   cd pynenc
   ```

2. **Install UV**: Pynenc uses [UV](https://github.com/astral-sh/uv) for dependency management. Install it following the [UV installation guide](https://docs.astral.sh/uv/getting-started/installation/).

3. **Install Dependencies**: Install all dependencies including development and documentation extras:

   ```bash
   make install
   ```

4. **Install Pre-commit Hooks**: Set up automatic code quality checks:

   ```bash
   uv run pre-commit install
   ```

5. **Install Docker** (optional): Required for running integration tests with Redis, MongoDB, or RabbitMQ backends. Visit https://docs.docker.com/get-docker/.

6. **Start Development**: Make changes, commit, and push to your fork.

   ```{attention}
   Python docstrings are rendered by MyST and autodoc2. Use Markdown formatting in docstrings.
   See the [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html)
   and [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html).
   ```

## Running Tests

Run tests using Makefile targets or UV directly:

- **Unit tests**:

  ```bash
  make test-unit
  ```

- **Integration tests**:

  ```bash
  make test-integration
  ```

- **All tests with combined coverage**:

  ```bash
  make test
  ```

- **Run tests directly**:

  ```bash
  uv run pytest pynenc_tests/unit
  uv run pytest pynenc_tests/integration
  ```

## Checking Coverage

Generate coverage reports after running tests:

```bash
uv run coverage run -m pytest pynenc_tests/
uv run coverage report
uv run coverage html
```

Open `htmlcov/index.html` in a browser to view the detailed HTML report.

Maintain or improve test coverage with your contributions. Add tests for new code and bug fixes.

## Creating Pull Requests

1. Create a feature branch from `main`
2. Make your changes with clear, atomic commits following [conventional commit](https://www.conventionalcommits.org/) format
3. Ensure all tests pass: `make test`
4. Push to your fork and open a pull request
