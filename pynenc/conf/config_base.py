from abc import ABC
import os
from typing import Optional, Callable, Generic, TypeVar, Type, Any, cast, Iterator

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
        except (ValueError, TypeError):
            raise TypeError(f"Invalid type. Expected {expected_type}.")
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


class ConfigBase(ABC):
    """
    Ways of determining the config field value:
    (0 for max priority)
    0.- User sets the config field directly (not recommended)
    1.- User specifies environment variables
    2.- User specifies the location of the config file by env vars
    3.- User specifies the config filepath(ref to a yml, toml or json…)
    4.- User specifies the config by values (dict[str: Any])
    5.- User do not specify anything [default values]
    """

    def __init__(
        self,
        config_id: Optional[str] = None,
        config_values: Optional[dict[str, Any]] = None,
        config_filepath: Optional[str] = None,
    ) -> None:
        _ = avoid_multi_inheritance_field_conflict(self.__class__)
        self.config_id = (
            config_id or self.__class__.__name__.replace("Config", "").lower()
        )
        # 4.- User specifies the config by values (dict[str: Any])
        if config_values:
            self.init_config_value_from_mapping(config_values)
        # 3.- User specifies the config filepath(ref to a yml, toml or json…)
        if config_filepath:
            self.init_config_value_from_mapping(files.load_file(config_filepath))
        # 2.- User specifies the location of the config file by env vars
        # 2.1 Global config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH)):
            self.init_config_value_from_mapping(files.load_file(filepath))
        # 2.2 Specific class config filepath specify by env var
        if filepath := os.environ.get(get_env_key(ENV_FILEPATH, self)):
            self.init_config_value_from_mapping(files.load_file(filepath))
        # 1.- User specifies environment variables
        self.init_config_value_from_env_vars()

    @property
    def map_key(self) -> str:
        return self.config_id or self.__class__.__name__

    def init_config_value_from_mapping(self, mapping: dict[str, Any]) -> None:
        conf_mapping = mapping.get(self.map_key, {})
        conf_mapping = conf_mapping if isinstance(conf_mapping, dict) else {}
        for key in get_config_fields(self.__class__):
            if key in conf_mapping:
                # todo logging
                setattr(self, key, conf_mapping[key])
            elif key in mapping:
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
    """"""
    config_fields: dict[str, str] = {}
    for parent in config_cls.__bases__:
        config_fields.update(avoid_multi_inheritance_field_conflict(parent))
        for key in get_config_fields(parent):
            if key in config_fields:
                raise ConfigMultiInheritanceError(
                    f"ConfigField {key} found in parent classes {parent.__name__} and {config_fields[key]}"
                )
            config_fields[key] = parent.__name__
    return config_fields


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
