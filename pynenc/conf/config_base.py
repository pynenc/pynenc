import os
from typing import Any, Callable, Generic, Iterator, Optional, Type, TypeVar, cast

from ..exceptions import ConfigMultiInheritanceError
from ..util import files

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
            raise TypeError(f"Invalid type. Expected {expected_type}.") from ex
    else:
        raise TypeError(f"Cannot convert to {expected_type}")


class ConfigField(Generic[T]):
    """define each field from a config option"""

    def __init__(
        self, default_value: T, mapper: Optional[ConfigFieldMapper] = None
    ) -> None:
        self._value: T = default_value
        self._mapper = mapper or default_config_field_mapper

    def __get__(self, instance: object, owner: Type[object]) -> T:
        del instance, owner
        return self._value

    def __set__(self, instance: object, value: Any) -> None:
        del instance
        self._value = self._mapper(value, type(self._value))


ENV_PREFIX = "PYNENC"
ENV_SEP = "__"
ENV_FILEPATH = "FILEPATH"


def get_env_key(field: str, config: Optional["ConfigBase"] = None) -> str:
    """gets the key used in the environment variables"""
    if config:
        return f"{ENV_PREFIX}{ENV_SEP}{config.__class__.__name__.upper()}{ENV_SEP}{field.upper()}"
    return f"{ENV_PREFIX}{ENV_SEP}{field.upper()}"


class ConfigBase:
    """
    Ways of determining the config field value:
    (0 for max priority)
    0.- User sets the config field directly in the config instance (not recommended)
    1.- User specifies environment variables
    2.- User specifies the location of the config file by env vars
    3.- User specifies the config filepath(ref to a yml, toml or json…)
    4.- User specifies config values in pyproject.toml
    5.- User specifies the config by values (dict[str: Any])
    6.- Previous steps for any Parent config class
    7.- User do not specify anything [default values]
    """

    def __init__(
        self,
        config_values: Optional[dict[str, Any]] = None,
        config_filepath: Optional[str] = None,
    ) -> None:
        _ = avoid_multi_inheritance_field_conflict(self.__class__)
        # on the first run, we load defaults values specified in the mapping
        # afterwards, that values will be modified by the ancestors
        # the childs will have higher priority
        self._first_run = True
        self.init_config_values(self.__class__, config_values, config_filepath)

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
            self.init_config_value_from_mapping(config_id, config_values)
        # 4.- User specifies config values in pyproject.toml
        if os.path.isfile("pyproject.toml"):
            self.init_config_value_from_mapping(
                config_id, files.load_file("pyproject.toml")
            )
        # 3.- User specifies the config filepath(ref to a yml, toml or json…)
        if config_filepath:
            self.init_config_value_from_mapping(
                config_id, files.load_file(config_filepath)
            )
        # 2.- User specifies the location of the config file by env vars
        # 2.1 Global config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH)):
            self.init_config_value_from_mapping(config_id, files.load_file(filepath))
        # 2.2 Specific class config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH, self)):
            self.init_config_value_from_mapping(config_id, files.load_file(filepath))
        # 1.- User specifies environment variables
        self.init_config_value_from_env_vars()
        # after the first run, we do not consider default values anymore
        # because they are the same for each relative class and could overwrite values
        self._first_run = False

    def init_config_value_from_mapping(
        self, config_id: str, mapping: dict[str, Any]
    ) -> None:
        conf_mapping = mapping.get(config_id, {})
        conf_mapping = conf_mapping if isinstance(conf_mapping, dict) else {}
        for key in get_config_fields(self.__class__):
            if key in conf_mapping:
                # todo logging
                setattr(self, key, conf_mapping[key])
            elif self._first_run and key in mapping:
                # todo logging
                setattr(self, key, mapping[key])

    def init_config_value_from_env_vars(self) -> None:
        for key in get_config_fields(self.__class__):
            if get_env_key(key, self) in os.environ:
                setattr(self, key, os.environ[get_env_key(key, self)])
            elif get_env_key(key) in os.environ:
                setattr(self, key, os.environ[get_env_key(key)])


def get_config_fields(cls: Type) -> Iterator[str]:
    for key, value in cls.__dict__.items():
        if isinstance(value, ConfigField):
            yield key


def avoid_multi_inheritance_field_conflict(config_cls: Type) -> dict[str, str]:
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
    for parent in config_cls.__bases__:
        if not issubclass(parent, ConfigBase) or parent is ConfigBase:
            continue
        for key in get_config_fields(parent):
            if key in map_field_to_config_cls:
                raise ConfigMultiInheritanceError(
                    f"ConfigField {key} found in parent classes {parent.__name__} and {map_field_to_config_cls[key]}"
                )
            map_field_to_config_cls[key] = parent.__name__
        # add current parent ancestor's fields that may not be specified in the current class
        map_field_to_config_cls.update(avoid_multi_inheritance_field_conflict(parent))
    return map_field_to_config_cls


def get_config_family_fields(config_cls: Type) -> dict[str, str]:
    """"""
    config_fields: dict[str, str] = {}
    for key, value in config_cls.__dict__.items():
        if isinstance(value, ConfigField):
            if key in config_fields:
                raise ConfigMultiInheritanceError(
                    f"ConfigField {key} found in parent classes {config_cls.__name__} and {config_fields[key]}"
                )
            config_fields[key] = config_cls.__name__
    return config_fields


# Config requirements

# It should accept:- yaml files
# - Toml files
# - Yaml files
# - Json files
# - Environment variables

# Collisions
# - Value priority (max to min): Env variables, file, config_values, defaults

# ENV VARS Naming conventions
# - ENV_VARS all start by “PYNENC_“ followed by config class name or config_id

# Flexibility
# - The user can create it’s own component (subclass of BaseOrchestrator, BaseBroker, etc…)
# - Then he just have to specify his class in the Pynenc app
# - But, how can he extend the config as easily?
#     - One config class per component? Sounds cumbersome

# Ex of flexible config, on the yaml file:
# 	# root level pynenc
# 	test_mode: true
# 	# config for Base Orchestrator, common to all classes
# 	# (or used in the base code, non abstract methods)
# 	BaseOrchestrator:
# 		host: localhost            -> auto env: PYNENC__BASE-ORCHESTRATOR__host
# 	# specific configs for each subclass of Orchestrator, it can modify the parent
# 	RedisOrchestrator:
# 		host: redis                -> auto env: PYNENC__REDIS-ORCHESTRATOR__host
# 	# subclasses can have specific values
# 	MemOrchestrator:
# 		mock_host: true        -> auto env: PYNENC__MEM-ORCHESTRATOR__mock_host
