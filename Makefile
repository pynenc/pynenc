.PHONY: install clean test test-unit test-integration coverage htmlcov pre-commit combine-coverage

# Default Python version used in the project
PYTHON_VERSION := 3.11.7

install:
	@echo "Installing dependencies via UV with all extras..."
	uv sync --all-extras

install-pre-commit: install
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install

pre-commit:
	@echo "Running pre-commit on all files..."
	uv run pre-commit run --all-files

clean:
	@echo "Cleaning previous coverage data and HTML reports..."
	rm -f .coverage .coverage.*
	rm -rf htmlcov

test-unit: clean
	@echo "Running unit tests with coverage..."
	uv run coverage run -m pytest pynenc_tests/unit
	uv run coverage report
	uv run coverage html --show-contexts --title "Unit Test Coverage"

test-integration: clean
	@echo "Running integration tests with coverage..."
	uv run coverage run -m pytest pynenc_tests/integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Integration Test Coverage"

test: clean
	@echo "Running all tests with combined coverage..."
	uv run coverage erase
	uv run coverage run -m pytest pynenc_tests/unit
	uv run coverage run --append -m pytest pynenc_tests/integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Combined Test Coverage"

# This target matches the GitHub Actions flow by creating separate coverage files
test-ci: clean
	@echo "Running tests in CI mode (separate coverage files)..."
	uv run coverage run -m pytest tests/unit
	cp .coverage coverage.unit
	uv run coverage run -m pytest tests/integration
	cp .coverage coverage.integration
	$(MAKE) combine-coverage

combine-coverage:
	@echo "Combining coverage data..."
	uv run coverage combine coverage.unit coverage.integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Combined Test Coverage"

coverage:
	@echo "Displaying coverage report..."
	uv run coverage report

htmlcov:
	@echo "Generating HTML coverage report..."
	uv run coverage html --show-contexts --title "Coverage Report"
