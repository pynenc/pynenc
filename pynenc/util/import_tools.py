from collections.abc import Callable


def is_module_level_function(func: Callable) -> bool:
    """
    Check if a function is defined at module level.

    This ensures the function can be properly serialized and imported.

    :param func: The function to check
    :return: True if the function is defined at module level, False otherwise
    """
    if not callable(func):
        return False

    if func.__name__ == "<lambda>":
        return False

    if func.__qualname__ != func.__name__:
        return False

    if not hasattr(func, "__module__") or func.__module__ == "__main__":
        return False

    return True
