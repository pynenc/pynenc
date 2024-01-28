# Documentation Setup for Pynenc

This directory contains the Sphinx documentation for Pynenc. To build the documentation, you need to install the necessary dependencies using Poetry and then use Sphinx to build the documentation.

## Prerequisites

Ensure you have Poetry installed in your environment. Poetry handles dependency management and packaging in Python projects. If you haven't installed Poetry yet, visit the [official Poetry installation guide](https://python-poetry.org/docs/#installation) to set it up.

## Installing Documentation Dependencies

1. **Activate the Project Environment**: If you are using a virtual environment for your project, ensure it's activated. If you're not using one, it's recommended to create and activate one to avoid conflicts with global packages. You can activate the Poetry-managed virtual environment for your project by running:

   ```bash
   poetry shell
   ```

2. **Install Optional Documentation Dependencies**: Run the following command in the root directory of the project (where your `pyproject.toml` is located):

   ```bash
   poetry install --with docs
   ```

This command tells Poetry to install the optional dependencies listed under the [tool.poetry.group.doc.dependencies] section in pyproject.toml.

## Building the Documentation

Once you have installed all the required dependencies, you can build the documentation:

1. **Navigate to the `docs` Directory**: Change your current directory to the `docs` subfolder:

   ```bash
   cd docs
   ```

2. **Build the Documentation**: Use the `make` utility to build the documentation:

   - On Unix-based systems (Linux, macOS), run:

     ```bash
     make html
     ```

   - On Windows, run:
     `bash .\make.bat html `
     This command will generate the HTML documentation in the `_build/html` directory.

   ```{tip}
   For examples check the [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html),
   [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html)
   and the complete directives references at [Myst docs](https://mystmd.org/guide/directives)
   ```

3. **View the Documentation**: Open the HTML files in the `_build/html` directory with a web browser to view the rendered documentation.

## Troubleshooting

- If you encounter any issues with missing dependencies or errors during the build process, ensure that all dependencies are correctly installed and that you are in the proper directory (where the Makefile is located).

- For detailed errors or troubleshooting, refer to the Sphinx documentation or check the console output for error messages.

For more information on how to `contribute` to the documentation, see the contributing section in the main project documentation.
