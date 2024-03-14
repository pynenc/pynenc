from cistell import ConfigBase

from pynenc.conf import constants


class ConfigPynencBase(ConfigBase):
    """Base Config for the Pynenc Application"""

    TOML_CONFIG_ID: str = constants.TOML_CONFIG_ID
    ENV_PREFIX: str = constants.ENV_PREFIX
    ENV_SEP: str = constants.ENV_SEPARATOR
    ENV_FILEPATH: str = constants.ENV_FILEPATH
    IGNORE_CLASS_NAME_SUBSTR: str = constants.IGNORE_CLASS_NAME_SUBSTR
