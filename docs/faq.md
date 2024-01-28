# Frequently Asked Questions

This FAQ addresses common questions and issues users may encounter with Pynenc.

## Why can't I define tasks in a module that can run as a script?

In Pynenc, tasks are not supported in modules that are intended to run as scripts. This includes any module executed directly (such as through `python script.py` or `python -m module`) where its `__name__` attribute is set to `"__main__"`.

The limitation arises because, when executed as the main program, the module name (`func.__module__`) is interpreted as `"__main__"`. In a distributed environment like Pynenc, where tasks are executed in worker processes, the `__main__` module refers to the worker itself, not the original script. Therefore, a task defined in such a module would be identified as `__main__.task_name` in the worker's context, leading to confusion and difficulties in correctly locating and executing the task. To ensure robustness and simplicity in task management, Pynenc does not support defining tasks in modules that are executed as the main program. Tasks should be defined in regular modules for proper identification and execution in a distributed setting.
