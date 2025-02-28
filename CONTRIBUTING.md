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

3. **Install Docker**: Ensure you have Docker installed on your system as it might be used for running services such as databases or other dependencies.

- Docker Installation: [Get Docker](https://docs.docker.com/get-docker/)

4. **Install Poetry**: pynenc uses Poetry for dependency management. Install Poetry as per the official documentation: [Poetry Installation](https://python-poetry.org/docs/#installation).

5. **Set Up the Project**: Inside the project directory, set up your local development environment using Poetry. This will install all dependencies, including those needed for development.

```bash
poetry install
```

6. **Install Pre-commit Hooks**: Set up pre-commit hooks in your local repository.

```bash
poetry run pre-commit install
```

7. **Activate the Virtual Environment**: Use Poetry to activate the virtual environment.

```bash
poetry shell
```

8. **Start Development**: You're now ready to start development. Feel free to make changes, commit them, and push them to your fork.

## Running Tests and Checking Coverage

Maintaining a high standard of code quality is crucial, which includes thorough testing and good test coverage. Here's how to run tests and check coverage:

1. **Running Tests**: After setting up your development environment, run the tests to ensure everything works as expected.

The integration tests require an instance of redis running, eg:

```bash
docker run --name redis -e REDIS_PASSWORD=password -p 6379:6379 -d redis:7.4-rc
```

Start the test in poetry

```bash
poetry run pytest
```

2. **Checking Test Coverage**: Use the `coverage` tool to check the code coverage by tests.

- Run the tests with coverage tracking:
  ```bash
  poetry run coverage run -m pytest
  ```
- Generate a coverage report. You can view the coverage report in two ways:
  - For a console summary:
    ```bash
    poetry run coverage report
    ```
  - For a detailed HTML report:
    ```bash
    poetry run coverage html
    ```
    This generates a report in the `htmlcov` directory. Open `htmlcov/index.html` in a web browser to view it.

Remember, these guidelines will become relevant once the project is open for contributions. Until then, feel free to familiarize yourself with the codebase and the project's goals.
