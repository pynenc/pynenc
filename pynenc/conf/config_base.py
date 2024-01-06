import os
from collections import defaultdict
from typing import Any, Callable, Generic, Iterator, Optional, Type, TypeVar, cast

from ..exceptions import ConfigMultiInheritanceError
from ..util import files
from .constants import ENV_FILEPATH, ENV_PREFIX, ENV_SEP

T = TypeVar("T")

ConfigLoader = Callable[[str], dict[str, str]]
ConfigFieldMapper = Callable[[Any, Type[T]], T]


def default_config_field_mapper(value: Any, expected_type: Type[T]) -> T:
    if isinstance(value, expected_type):
        return value
    if callable(expected_type):
        try:
            callable_type = cast(Callable[[Any], T], expected_type)
            return callable_type(value)  # type conversion
        except (ValueError, TypeError) as ex:
            raise TypeError(
                f"Invalid type. Expected {expected_type} instead {type(value)}."
            ) from ex
    else:
        raise TypeError(f"Cannot convert to {expected_type}")


class ConfigField(Generic[T]):
    """
    Define each typed field from a ConfigBase instance.

    This class is used to define typed configuration fields within a ConfigBase
    subclass. It ensures type consistency and supports value validation and casting.

    Parameters
    ----------
    default_value : T
        The default value for the configuration field.
    mapper : Optional[ConfigFieldMapper]
        An optional function to map or transform the value.

    Attributes
    ----------
    _default_value : T
        Stores the default value of the configuration field.
    _mapper : ConfigFieldMapper
        The function used for mapping or transforming the value.
    """

    def __init__(
        self, default_value: T, mapper: Optional[ConfigFieldMapper] = None
    ) -> None:
        self._default_value: T = default_value
        self._mapper = mapper or default_config_field_mapper

    def __get__(self, instance: Optional["ConfigBase"], owner: Type[object]) -> T:
        del owner
        if instance is None:
            return self._default_value
        return instance._config_values.get(self, self._default_value)

    def __set__(self, instance: "ConfigBase", value: Any) -> None:
        instance._config_values[self] = self._mapper(value, type(self._default_value))


def get_env_key(field: str, config: Optional[Type["ConfigBase"]] = None) -> str:
    """gets the key used in the environment variables"""
    if config:
        return f"{ENV_PREFIX}{ENV_SEP}{config.__name__.upper()}{ENV_SEP}{field.upper()}"
    return f"{ENV_PREFIX}{ENV_SEP}{field.upper()}"


class ConfigBase:
    """
    Base class for defining configuration settings.

    This class serves as the base for creating configuration classes. It supports
    hierarchical and flexible configuration from various sources, including
    environment variables, configuration files, and default values.

    Configuration values are determined based on the following priority (highest to lowest):
    1. Direct assignment in the config instance (not recommended)
    2. Environment variables
    3. Configuration file path specified by environment variables
    4. Configuration file path (YAML, TOML, JSON) by config_filepath parameter
    5. `pyproject.toml`
    6. Default values specified in the `ConfigField`
    7. Previous steps for any Parent config class
    8. User does not specify anything (default values)

    Examples
    --------
    Define a configuration class for a Redis client:

    .. code-block:: python

        class ConfigRedis(ConfigBase):
            redis_host = ConfigField("localhost")
            redis_port = ConfigField(6379)
            redis_db = ConfigField(0)

    Define a main configuration class for orchestrator components:

    .. code-block:: python

        class ConfigOrchestrator(ConfigBase):
            cycle_control = ConfigField(True)
            blocking_control = ConfigField(True)
            auto_final_invocation_purge_hours = ConfigField(24.0)

    Combine configurations using multiple inheritance:

    .. code-block:: python

        class ConfigOrchestratorRedis(ConfigOrchestrator, ConfigRedis):
            pass

    The `ConfigOrchestratorRedis` class now includes settings from both `ConfigOrchestrator`
    and `ConfigRedis`.
    """

    def __init__(
        self,
        config_values: Optional[dict[str, Any]] = None,
        config_filepath: Optional[str] = None,
    ) -> None:
        self.config_cls_to_fields: dict[str, set[str]] = defaultdict(set)
        _ = avoid_multi_inheritance_field_conflict(
            self.__class__, self.config_cls_to_fields
        )
        self._config_values: dict[ConfigField, Any] = {}

        # on the first run, we load defaults values specified in the mapping
        # afterwards, that values will be modified by the ancestors
        # the childs will have higher priority
        self._mapped_keys: set[str] = set()
        self.init_config_values(self.__class__, config_values, config_filepath)

    @classmethod
    def config_fields(cls) -> list[str]:
        return list(get_config_fields(cls))

    @property
    def all_fields(self) -> list[str]:
        return list(set().union(*self.config_cls_to_fields.values()))

    @staticmethod
    def get_config_id(config_cls: Type["ConfigBase"]) -> str:
        return config_cls.__name__.replace("Config", "").lower()

    def init_parent_values(
        self,
        config_cls: Type["ConfigBase"],
        config_values: Optional[dict[str, Any]],
        config_filepath: Optional[str],
    ) -> None:
        # Initialize parent classes that are subclasses of ConfigBase
        for parent in config_cls.__bases__:
            if issubclass(parent, ConfigBase) and parent is not ConfigBase:
                self.init_config_values(parent, config_values, config_filepath)

    def init_config_values(
        self,
        config_cls: Type["ConfigBase"],
        config_values: Optional[dict[str, Any]],
        config_filepath: Optional[str],
    ) -> None:
        config_id = self.get_config_id(config_cls)
        self.init_parent_values(config_cls, config_values, config_filepath)
        if not get_config_fields(self.__class__):
            raise TypeError(
                "Cannot instantiate a ConfigBase without any ConfigField attribute"
            )
        # 5.- User specifies the config by values (dict[str: Any])
        if config_values:
            self.init_config_value_from_mapping(
                "config_values", config_id, config_values
            )
        # 4.- User specifies config values in pyproject.toml
        if os.path.isfile("pyproject.toml"):
            self.init_config_value_from_mapping(
                "pyproject.toml", config_id, files.load_file("pyproject.toml")
            )
        # 3.- User specifies the config filepath(ref to a yml, toml or json…)
        if config_filepath:
            self.init_config_value_from_mapping(
                "config_filepath", config_id, files.load_file(config_filepath)
            )
        # 2.- User specifies the location of the config file by env vars
        # 2.1 Global config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH)):
            self.init_config_value_from_mapping(
                "ENV_FILEPATH", config_id, files.load_file(filepath)
            )
        # 2.2 Specific class config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH, config_cls)):
            self.init_config_value_from_mapping(
                "ENV_CLASS_FILEPATH", config_id, files.load_file(filepath)
            )
        # 1.- User specifies environment variables
        self.init_config_value_from_env_vars(config_cls)

    def init_config_value_from_mapping(
        self, source: str, config_id: str, mapping: dict[str, Any]
    ) -> None:
        conf_mapping = mapping.get(config_id, {})
        conf_mapping = conf_mapping if isinstance(conf_mapping, dict) else {}
        for key in self.config_cls_to_fields.get(self.__class__.__name__, []):
            self.init_config_value_key_from_mapping(
                source, config_id, key, mapping, conf_mapping
            )

    def init_config_value_key_from_mapping(
        self, source: str, config_id: str, key: str, mapping: dict, conf_mapping: dict
    ) -> None:
        general_key = f"{source}##{key}"
        class_key = f"{source}##{config_id}##{key}"
        if general_key not in self._mapped_keys and key in mapping:
            setattr(self, key, mapping[key])
            self._mapped_keys.add(general_key)
        if class_key not in self._mapped_keys and key in conf_mapping:
            setattr(self, key, conf_mapping[key])
            self._mapped_keys.add(class_key)

    def init_config_value_from_env_vars(self, config_cls: Type["ConfigBase"]) -> None:
        for key in self.config_cls_to_fields.get(config_cls.__name__, []):
            if get_env_key(key, config_cls) in os.environ:
                setattr(self, key, os.environ[get_env_key(key, config_cls)])
            elif get_env_key(key) in os.environ:
                setattr(self, key, os.environ[get_env_key(key)])


def get_config_fields(cls: Type) -> Iterator[str]:
    for key, value in cls.__dict__.items():
        if isinstance(value, ConfigField):
            yield key


def avoid_multi_inheritance_field_conflict(
    config_cls: Type, config_cls_to_fields: dict[str, set[str]]
) -> dict[str, str]:
    """
    Ensures that the same configuration field is not defined in multiple parent classes of a given configuration class.

    This function checks all parent classes of the provided configuration class that are subclasses of `ConfigBase`.
    It ensures that each configuration field is defined only once among all parent classes. If a field is found in
    multiple parent classes, a `ConfigMultiInheritanceError` is raised. This check ensures deterministic behavior
    in the configuration inheritance hierarchy.

    Parameters:
        config_cls (Type): The configuration class to check for field conflicts.

    Returns:
        dict[str, str]: A dictionary mapping each configuration field to the name of the parent class where it is defined.

    Raises:
        ConfigMultiInheritanceError: If a configuration field is found in multiple parent classes.

    Example:
        >>> class ParentConfig1(ConfigBase):
        ...     field1 = ConfigField(default_value=1)
        ...
        >>> class ParentConfig2(ConfigBase):
        ...     field2 = ConfigField(default_value=2)
        ...
        >>> class ChildConfig(ParentConfig1, ParentConfig2):
        ...     pass
        ...
        >>> avoid_multi_inheritance_field_conflict(ChildConfig)
        {'field1': 'ParentConfig1', 'field2': 'ParentConfig2'}
    """
    map_field_to_config_cls: dict[str, str] = {}
    cls_fields: set[str] = set()
    for parent in config_cls.__bases__:
        if not issubclass(parent, ConfigBase) or parent is ConfigBase:
            continue
        for key in get_config_fields(parent):
            if key in map_field_to_config_cls:
                raise ConfigMultiInheritanceError(
                    f"ConfigField {key} found in parent classes {parent.__name__} and {map_field_to_config_cls[key]}"
                )
            map_field_to_config_cls[key] = parent.__name__
            config_cls_to_fields[parent.__name__].add(key)
        # add current parent ancestor's fields that may not be specified in the current class
        map_field_to_config_cls.update(
            avoid_multi_inheritance_field_conflict(parent, config_cls_to_fields)
        )
        cls_fields = cls_fields.union(config_cls_to_fields[parent.__name__])
    config_cls_to_fields[config_cls.__name__] = cls_fields.union(
        set(get_config_fields(config_cls))
    )
    return map_field_to_config_cls
