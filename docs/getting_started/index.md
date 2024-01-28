# Getting Started

This section covers the basics of getting Pynenc installed and running in your environment.

## Installation

Installing Pynenc is straightforward and can be accomplished using pip. Simply run the following command in your terminal:

```bash
pip install pynenc
```

This command will download and install Pynenc along with its necessary dependencies. Once the installation is complete, you are ready to start using Pynenc in your Python projects.

For more detailed instructions and advanced installation options, refer to the `Installation` section in the Pynenc Documentation.

## Quick Start

To get a quick feel of Pynenc, here's a basic example of creating and executing a distributed task.

1. **Define a Task**: Start by defining a simple task with the `@app.task` decorator:

   ```{code-block} python
    from pynenc import Pynenc

    app = Pynenc()

    @app.task
    def add(x: int, y: int) -> int:
        add.logger.info(f"{add.task_id=} Adding {x} + {y}")
        return x + y
   ```

2. **Execute the Task**: Now execute the task and retrieve the result:
   ```{code-block} python
    result = add(1, 2).result
    print(result)  # Outputs: 3
   ```

For a more comprehensive guide on setting up and running this example, visit our [Basic Redis Example on GitHub](https://github.com/pynenc/samples/tree/main/basic_redis_example).

```{important}
   Note that in Pynenc, tasks cannot be defined in Python modules intended to run as standalone scripts
   (where `__name__` is set to `"__main__"`). This includes modules executed directly or using the
   `python -m module` command. To learn more about this limitation and its implications, refer to the
   {doc}`../faq` or the detailed explanation in the {doc}`../usage_guide/index`.
```

## Requirements

To effectively use Pynenc in a distributed system, the primary requirement is:

- **Redis**: Currently, Pynenc requires a Redis server for distributed task management. Make sure Redis is installed and running in your environment.

Future Updates:

- Development plans include extending support to additional databases and message queues to broaden Pynenc's compatibility in various distributed systems.
