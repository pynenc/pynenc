from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigStateBackend(ConfigPynencBase):
    """Main config of the state backend components"""


class ConfigStateBackendSQLite(ConfigStateBackend, ConfigSQLite):
    """SQLite-based state backend configuration"""
