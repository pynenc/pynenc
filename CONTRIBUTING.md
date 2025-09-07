# Contributing to pynenc

Thank you for your interest in contributing to pynenc! At this moment, the project is in its initial development phase and is not yet ready for contributions from the community. We are working hard to reach a Minimum Viable Product (MVP) and establish a solid foundation for the project.

Once the project reaches a stage where we are ready to accept contributions, we will update this CONTRIBUTING guide with detailed instructions on how to contribute. In the meantime, feel free to watch the repository for updates and engage in discussions in the issues.

We appreciate your understanding and look forward to collaborating with you in the near future!

Best regards,
Pynenc

## Setting Up the Development Environment

To contribute to pynenc once it's open for contributions, follow these steps to set up your development environment:

1. **Fork the Repository**: Start by forking the pynenc repository on GitHub to your own account.

2. **Clone the Fork**: Clone your fork to your local machine.

```bash
git clone https://github.com/YOUR_USERNAME/pynenc.git
cd pynenc
```

3. **Install Poetry**: pynenc uses Poetry for dependency management. Install Poetry as per the official documentation: [Poetry Installation](https://python-poetry.org/docs/#installation).

4. **Set Up the Project**: Inside the project directory, set up your local development environment using Poetry and install pre-commit hooks.

```bash
# Install dependencies and set up the project
make install

# Install pre-commit hooks
make install-pre-commit
```

5. **Activate the Virtual Environment**: Use Poetry to activate the virtual environment.

```bash
poetry shell
```

6. **Start Development**: You're now ready to start development. Feel free to make changes, commit them, and push them to your fork.

## Running Tests and Checking Coverage

Maintaining a high standard of code quality is crucial, which includes thorough testing and good test coverage. Here's how to run tests and check coverage:

### Running Tests

You can run different test suites using the following make commands:

```bash
# Run all tests with combined coverage
make test

# Run only unit tests
make test-unit

# Run only integration tests
make test-integration
```

### Checking Test Coverage

After running tests, you can check coverage in multiple ways:

```bash
# Display coverage report in the console
make coverage

# Generate an HTML coverage report
make htmlcov
```

The HTML report will be available at `htmlcov/index.html`. Open it in a web browser to view detailed coverage information.

## Pre-commit Hooks

The project uses pre-commit hooks to ensure code quality and consistency. These hooks run automatically when you commit changes, but you can also run them manually:

```bash
make pre-commit
```

## Continuous Integration

The project uses GitHub Actions for continuous integration. The CI pipeline runs:

1. Pre-commit checks
2. Unit tests
3. Integration tests
4. Combines coverage reports

When you submit a pull request, all these checks will run automatically. Make sure your changes pass all these checks before requesting a review.

Remember, these guidelines will become relevant once the project is open for contributions. Until then, feel free to familiarize yourself with the codebase and the project's goals.
