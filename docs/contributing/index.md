# Contributing to Pynenc

Thank you for your interest in contributing to Pynenc! At this moment, the project is in its initial development phase and is not yet open for external contributions. We are diligently working towards a Minimum Viable Product (MVP) and establishing a stable foundation.

Once the project reaches a stage where community contributions can be integrated, this guide will be updated with detailed instructions on the contribution process. For now, we invite you to watch the repository for updates and participate in issue discussions.

We appreciate your understanding and look forward to your contributions in the future!

Best regards,
The Pynenc Team

```{toctree}
:hidden:
:maxdepth: 2
:caption: Detailed Use Cases

./docs
```

## Setting Up the Development Environment

To contribute to Pynenc once it's open for contributions, follow these typical steps to set up your development environment:

1. **Fork the Repository**: Start by forking the Pynenc repository (https://github.com/pynenc/pynenc) on GitHub to your own account.

2. **Clone the Fork**: Clone your fork to your local machine.

   ```bash
      git clone https://github.com/pynenc/pynenc.git
      cd pynenc
   ```

3. **Install Docker**: Make sure you have Docker installed on your system as it may be used for running services such as databases or other dependencies.

   - Docker Installation: Visit https://docs.docker.com/get-docker/

4. **Install Poetry**: Pynenc uses Poetry for dependency management. Install Poetry using the recommended method from the official documentation at https://python-poetry.org/docs/#installation.

5. **Set Up the Project**: Inside the project directory, set up your local development environment using Poetry. This will install all dependencies, including those needed for development.

   ```bash
      poetry install
   ```

6. **Install Pre-commit Hooks**: After installing all dependencies, set up pre-commit hooks in your local repository. This ensures that code quality checks are automatically performed before each commit.

   ```bash
      poetry run pre-commit install
   ```

7. **Activate the Virtual Environment**: Use Poetry to activate the virtual environment.

   ```bash
      poetry shell
   ```

8. **Start Development**: You are now ready to start development. Make changes, commit them, and push them to your fork.

   ```{attention}
   The python docstrings will be render by Myst and autodoc2, use markdown to document it and any Myst formatting allowed:
   For examples check the [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html),
   [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html)
   and the complete directives references at [Myst docs](https://mystmd.org/guide/directives)
   ```

9. **Creating Pull Requests**: Once the project is open for contributions, you will be able to create pull requests from your fork to the main Pynenc repository.

10. **Set Up Pre-commit Hooks**: To ensure code quality and consistency, set up pre-commit hooks in your local environment. These hooks will automatically check your commits for issues like formatting errors.

```bash
   poetry run pre-commit install
```

## Running Tests and Checking Coverage

Pynenc aims to maintain a high standard of code quality, which includes thorough testing and maintaining good test coverage. Here’s how you can run tests and check coverage:

1. **Running Tests**: After setting up your development environment, you can run the tests to ensure everything is working as expected.

   ```bash
      poetry run pytest
   ```

   This command will execute all the tests in the `tests` directory.

2. **Checking Test Coverage**: To check how much of the code is covered by tests, use the `coverage` tool.

   - First, run the tests with coverage tracking:
     ```bash
        poetry run coverage run -m pytest
     ```
   - Then, generate a coverage report. There are two ways to view the coverage report:

     - For a summary in the console, use:
       ```bash
          poetry run coverage report
       ```
     - For a more detailed HTML report, use:
       ```bash
          poetry run coverage html
       ```

     This will generate a report in the `htmlcov` directory. You can open `htmlcov/index.html` in a web browser to view it.

Please aim to maintain or improve the test coverage with your contributions. It’s recommended to add tests for any new code or when fixing bugs.
