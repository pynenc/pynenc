# Dockerfile
FROM python:3.11

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set the PATH to include Poetry
ENV PATH="${PATH}:/root/.local/bin"

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Install dependencies without creating a virtual environment
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction

# Run tests
CMD ["poetry", "run", "pytest", "tests/integration"]
