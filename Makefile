.PHONY: install
install:
	@echo "Installing dependencies via UV with all extras..."
	uv sync --all-extras

.PHONY: install-pre-commit
install-pre-commit: install
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install

.PHONY: pre-commit
pre-commit:
	@echo "Running pre-commit on all files..."
	uv run pre-commit run --all-files

.PHONY: clean
clean:
	@echo "Cleaning previous coverage data and HTML reports..."
	rm -f .coverage .coverage.*
	rm -rf htmlcov

.PHONY: test-unit
test-unit: clean
	@echo "Running unit tests with coverage..."
	uv run coverage run -m pytest pynenc_tests/unit
	uv run coverage report
	uv run coverage html --show-contexts --title "Unit Test Coverage"

.PHONY: test-integration
test-integration: clean
	@echo "Running integration tests with coverage..."
	uv run coverage run -m pytest pynenc_tests/integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Integration Test Coverage"

.PHONY: test
test: clean
	@echo "Running all tests with combined coverage..."
	uv run coverage erase
	uv run coverage run -m pytest pynenc_tests/unit
	uv run coverage run --append -m pytest pynenc_tests/integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Combined Test Coverage"

# This target matches the GitHub Actions flow by creating separate coverage files
.PHONY: test-ci
test-ci: clean
	@echo "Running tests in CI mode (separate coverage files)..."
	uv run coverage run -m pytest tests/unit
	cp .coverage coverage.unit
	uv run coverage run -m pytest tests/integration
	cp .coverage coverage.integration
	$(MAKE) combine-coverage

.PHONY: combine-coverage
combine-coverage:
	@echo "Combining coverage data..."
	uv run coverage combine coverage.unit coverage.integration
	uv run coverage report
	uv run coverage html --show-contexts --title "Combined Test Coverage"

.PHONY: coverage
coverage:
	@echo "Displaying coverage report..."
	uv run coverage report

.PHONY: htmlcov
htmlcov:
	@echo "Generating HTML coverage report..."
	uv run coverage html --show-contexts --title "Coverage Report"

.PHONY: clean-build
build: clean-build ## Build wheel file
	@echo "🚀 Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: publish
publish: ## Publish a release to PyPI.
	@echo "🚀 Publishing."
	@uvx twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
