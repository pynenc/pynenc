.PHONY: install clean test test-unit test-integration coverage htmlcov pre-commit docker-redis combine-coverage

# Default Python version used in the project
PYTHON_VERSION := 3.11.7

install:
	@echo "Installing dependencies via Poetry..."
	poetry install --no-interaction

install-pre-commit: install
	@echo "Installing pre-commit hooks..."
	poetry run pre-commit install

pre-commit:
	@echo "Running pre-commit on all files..."
	poetry run pre-commit run --all-files

clean:
	@echo "Cleaning previous coverage data and HTML reports..."
	rm -f .coverage .coverage.*
	rm -rf htmlcov

test-unit: clean
	@echo "Running unit tests with coverage..."
	poetry run coverage run -m pytest tests/unit
	poetry run coverage report
	poetry run coverage html --show-contexts --title "Unit Test Coverage"

test-integration: clean
	@echo "Running integration tests with coverage..."
	poetry run coverage run -m pytest tests/integration
	poetry run coverage report
	poetry run coverage html --show-contexts --title "Integration Test Coverage"

test: clean
	@echo "Running all tests with combined coverage..."
	poetry run coverage erase
	poetry run coverage run -m pytest tests/unit
	poetry run coverage run --append -m pytest tests/integration
	poetry run coverage report
	poetry run coverage html --show-contexts --title "Combined Test Coverage"

# This target matches the GitHub Actions flow by creating separate coverage files
test-ci: clean
	@echo "Running tests in CI mode (separate coverage files)..."
	poetry run coverage run -m pytest tests/unit
	cp .coverage coverage.unit
	poetry run coverage run -m pytest tests/integration
	cp .coverage coverage.integration
	$(MAKE) combine-coverage

combine-coverage:
	@echo "Combining coverage data..."
	poetry run coverage combine coverage.unit coverage.integration
	poetry run coverage report
	poetry run coverage html --show-contexts --title "Combined Test Coverage"

coverage:
	@echo "Displaying coverage report..."
	poetry run coverage report

htmlcov:
	@echo "Generating HTML coverage report..."
	poetry run coverage html --show-contexts --title "Coverage Report"

docker-redis:
	@echo "Starting Redis container for integration tests..."
	docker run --name redis-test -p 6379:6379 -d redis:7.2.3
	@echo "Redis container started. To stop it later, run: docker stop redis-test && docker rm redis-test"
