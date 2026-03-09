# Building the Documentation

Build and preview the Sphinx documentation for Pynenc locally.

## Prerequisites

Install all project dependencies (including documentation tools):

```bash
make install
```

The docs build commands (`make docs`, `make docs-serve`) automatically include the docs dependency group via `uv run --group docs`.

## Building

From the project root:

```bash
# Build HTML docs (output: docs/_build/html/index.html)
make docs

# Build and serve locally at http://localhost:8080
make docs-serve
```

Or manually from the `docs/` directory:

```bash
cd docs
make html
```

## Documentation Format

Pynenc documentation uses:

- **MyST Markdown** (`.md` files) as the primary format
- **autodoc2** for automatic API reference generation from Python docstrings
- **Sphinx** as the documentation build engine

Write docstrings in Markdown using MyST syntax. See:

- [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html)
- [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html)
- [MyST documentation](https://mystmd.org/guide/directives)

## Troubleshooting

- If you encounter missing dependency errors, run `make install` from the project root
- For Sphinx build errors, check the console output for specific file and line references
- Ensure you are in the `docs/` directory when running `make html` directly
