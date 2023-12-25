import os
import tempfile
from unittest.mock import patch

import pytest
import yaml

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType, ConfigTask
from pynenc.exceptions import InvalidTaskOptionsError

app = Pynenc()


class CustomException(Exception):
    pass


@app.task(
    auto_parallel_batch_size=1,
    retry_for=(CustomException,),
    max_retries=2,
    running_concurrency=ConcurrencyControlType.TASK,
    registration_concurrency=ConcurrencyControlType.KEYS,
    key_arguments=("id",),
    on_diff_non_key_args_raise=True,
)
def store_with_opt(id: int, value: int) -> None:
    del id, value
    pass


def test_task_config_from_decorator_options() -> None:
    """
    Test that the task config was set with the options from the decorator
    """
    assert store_with_opt.conf.auto_parallel_batch_size == 1
    assert store_with_opt.conf.retry_for == (CustomException,)  # type: ignore
    assert store_with_opt.conf.max_retries == 2
    assert store_with_opt.conf.running_concurrency == ConcurrencyControlType.TASK
    assert store_with_opt.conf.registration_concurrency == ConcurrencyControlType.KEYS
    assert store_with_opt.conf.key_arguments == ("id",)
    assert store_with_opt.conf.on_diff_non_key_args_raise is True


def test_all_config_field_checked() -> None:
    """
    Test that all the config fields are considered in this tests:
    - all the config fields specified in ConfigTask, should have a corresponding option in the task decorator
    """
    config_fields = ConfigTask.config_fields()
    option_fields = list(store_with_opt.conf.task_options.keys())
    assert option_fields
    assert len(config_fields) == len(option_fields)
    assert set(config_fields) == set(option_fields)


def test_options_serialization() -> None:
    """
    Test that the options can be serialized and deserialized
    """
    serialized = store_with_opt.conf.options_to_json()
    options = ConfigTask.options_from_json(serialized)
    assert options == store_with_opt.conf.task_options


@app.task
def store_with_env(id: int, value: int) -> None:
    del id, value
    pass


def test_task_config_with_env_vars() -> None:
    """
    Test that the task config was set with the options from the environment variables
    """
    with patch.dict(
        os.environ,
        {"PYNENC__CONFIGTASK__AUTO_PARALLEL_BATCH_SIZE": "2"},
    ):
        assert store_with_env.conf.auto_parallel_batch_size == 2


@app.task
def store_with_specific_env(id: int, value: int) -> None:
    del id, value
    pass


def test_task_config_with_task_specific_env_vars() -> None:
    """
    Test that the task config was set with the options from the environment variables
    but for a specific task 'test_task_config.store_with_env'
    """
    with patch.dict(
        os.environ,
        {
            "PYNENC__CONFIGTASK__AUTO_PARALLEL_BATCH_SIZE": "2",
            "PYNENC__CONFIGTASK__TEST_TASK_CONFIG__STORE_WITH_SPECIFIC_ENV__AUTO_PARALLEL_BATCH_SIZE": "3",
        },
    ):
        assert store_with_specific_env.conf.auto_parallel_batch_size == 3


def test_task_config_from_file() -> None:
    """
    Test that the task config was set with the options from the file
    """
    fd, filepath = tempfile.mkstemp(suffix=".yaml")
    content = yaml.dump(
        {
            "task": {
                # general config values for all the task
                "auto_parallel_batch_size": 4,
                "max_retries": 10,
                # specific config values for the task module_name.task_name
                "module_name.task_name": {"max_retries": 5},
            },
        }
    )
    with os.fdopen(fd, "w") as tmp:
        tmp.write(content)
    all_tasks_config = ConfigTask(
        task_id="module_name.random_task", config_filepath=filepath
    )
    assert all_tasks_config.auto_parallel_batch_size == 4
    assert all_tasks_config.max_retries == 10
    one_task_config = ConfigTask(
        task_id="module_name.task_name", config_filepath=filepath
    )
    assert one_task_config.auto_parallel_batch_size == 4
    assert one_task_config.max_retries == 5


def test_exception_on_wrong_options() -> None:
    """
    if the option do not match any value on the task config it will raise an exception
    """
    with pytest.raises(InvalidTaskOptionsError) as exc_info:
        app.task(non_existing_option=1)(print)

    assert (
        str(exc_info.value)
        == "InvalidTaskOptionsError(builtins.print): Invalid options: ['non_existing_option']"
    )
