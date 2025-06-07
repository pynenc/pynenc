# Pynmon Integration Tests

Integration tests for pynmon with built-in debugging support.

## Quick Debug Mode

To debug tests with browser access:

1. **Enable debug mode**:
   - **Option A**: In `test_*.py`, change `KEEP_ALIVE = 0` to `KEEP_ALIVE = 1`
   - **Option B**: Set environment variable: `export PYNMON_KEEP_ALIVE=1`
2. **Run any test**: Use VS Code's test runner or run: `pytest tests/integration/pynmon/test_home_integration.py::test_home_page_renders_successfully -s`
3. **Open browser**: Go to <http://localhost:8081> while test is running
4. **Debug**: Set breakpoints in test or server code and debug interactively
5. **Stop**: Press Ctrl+C when done debugging

### Shell Testing with Environment Variable

For easy shell-based testing, use the environment variable approach:

```bash
export PYNMON_KEEP_ALIVE=1
pytest tests/integration/pynmon/test_home_integration.py::test_home_page_renders_successfully -s
# Server stays alive for browser debugging at http://localhost:8081
```

## Features

- **Real HTTP server**: Tests use actual uvicorn server on port 8081 (browser accessible)
- **Same thread debugging**: Set breakpoints in both test and server code
- **Easy toggle**: Just change `KEEP_ALIVE = 0` to `KEEP_ALIVE = 1` for instant debugging
- **Auto cleanup**: Server stops automatically when debugging is complete

## Test Files

- `test_home_integration.py` - Complete home page integration tests with debug support
- `conftest.py` - Test fixtures and server setup

Happy debugging! 🐛🔍
