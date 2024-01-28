from typing import TypeVar

T = TypeVar("T", bound=object)  # Declare type variable


def get_all_subclasses(cls: type[T]) -> list[type[T]]:
    """
    Retrieves all subclasses of a given class.

    :param type[T] cls: The class to retrieve subclasses for.
    :return: A list of all subclasses.
    """
    all_subclasses = set()
    # remove all related type:ignores when mypy fix the issue
    # https://github.com/python/mypy/issues/4717
    # type: ignore # mypy issue #4717
    for subclass in cls.__subclasses__():
        all_subclasses.add(subclass)
        all_subclasses.update(get_all_subclasses(subclass))

    return list(all_subclasses)


def get_subclass(root_class: type[T], child_class_name: str) -> type[T]:
    """
    Retrieves a specific subclass by name from a root class.

    :param type[T] root_class: The root class.
    :param str child_class_name: The name of the subclass to retrieve.
    :return: the subclass with the given name (any level deep)
    """
    for subclass in get_all_subclasses(root_class):
        if subclass.__name__ == child_class_name:
            return subclass
    raise ValueError(f"Unknown subclass: {child_class_name} of {root_class.__name__}")
