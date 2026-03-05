# Building the Documentation

Build and preview the Sphinx documentation for Pynenc locally.

## Prerequisites

Install documentation dependencies using UV:

```bash
uv sync --all-extras
```

This installs Sphinx, MyST Parser, autodoc2, and all other documentation build dependencies.

## Building

1. Navigate to the `docs` directory:

   ```bash
   cd docs
   ```

2. Build the HTML documentation:

   ```bash
   make html
   ```

3. Open `_build/html/index.html` in a browser to preview.

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

- If you encounter missing dependency errors, run `uv sync --all-extras` from the project root
- For Sphinx build errors, check the console output for specific file and line references
- Ensure you are in the `docs/` directory when running `make html`
