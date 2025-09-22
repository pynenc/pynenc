import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Union

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.arguments import Arguments
    from pynenc.types import Args, Result

    # Type for the parallel function that generates arguments for parallel processing
    ParallelFuncReturn = Union[
        # Option 1: Just return an iterable of arguments (any format)
        Iterable[Union[tuple, dict, Arguments]],
        # Option 2: Return a tuple of (common_args, param_iter) for optimized processing of large shared data
        # This approach pre-serializes common_args once, reducing overhead for large arguments
        # tuple.0 Common arguments shared by all tasks
        # tuple.1 Iterable of dictionaries with task-specific arguments
        tuple[dict[str, Any], Iterable[dict]],
    ]
    ParallelFunc = Callable[[Args], ParallelFuncReturn]

    # Type for the aggregation function that combines results
    AggregateFunc = Callable[[Iterable[Result]], Result]


@dataclass(frozen=True)
class AppInfo:
    """
    Information about a Pynenc application instance.

    Stores metadata required for app discovery and re-instantiation.

    :param app_id: Unique identifier for the application
    :param module: Module path where the app is defined
    :param config_values: Configuration values for app initialization
    :param config_filepath: Path to configuration file, if any
    :param module_filepath: Absolute file path of the module
    :param app_variable: Name of the variable holding the app instance in the module
    """

    app_id: str
    module: str
    config_values: dict[str, Any] | None = None
    config_filepath: str | None = None
    module_filepath: str | None = None
    app_variable: str | None = None

    @classmethod
    def from_app(cls, app: "Pynenc") -> "AppInfo":
        """
        Create AppInfo from a Pynenc app instance.

        Captures necessary metadata for later re-instantiation.

        :param app: The Pynenc app instance
        :return: AppInfo containing app metadata
        """
        from pynenc.util.import_app import extract_module_info

        module_filepath, app_variable = extract_module_info(app)
        return cls(
            app_id=app.app_id,
            config_values=app.config_values,
            config_filepath=app.config_filepath,
            module=app.__module__,
            module_filepath=module_filepath,
            app_variable=app_variable,
        )

    def to_json(self) -> str:
        """
        Serialize AppInfo to JSON string.

        :return: JSON representation of AppInfo
        """
        return json.dumps(
            {
                "app_id": self.app_id,
                "config_values": self.config_values,
                "config_filepath": self.config_filepath,
                "module": self.module,
                "module_filepath": self.module_filepath,
                "app_variable": self.app_variable,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "AppInfo":
        """
        Deserialize AppInfo from JSON string.

        :param json_str: JSON string representation of AppInfo
        :return: AppInfo instance
        """
        data = json.loads(json_str)
        return cls(
            app_id=data["app_id"],
            config_values=data.get("config_values"),
            config_filepath=data.get("config_filepath"),
            module=data["module"],
            module_filepath=data.get("module_filepath"),
            app_variable=data.get("app_variable"),
        )
