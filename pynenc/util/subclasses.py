from typing import TypeVar

T = TypeVar("T")  # Declare type variable


def get_all_subclasses(cls: type[T]) -> list[type[T]]:
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses
