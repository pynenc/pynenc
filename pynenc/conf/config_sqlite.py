"""
SQLite-based configuration for Pynenc components.

Provides configuration classes that use SQLite as a shared backend for
reliable cross-process communication.
"""

from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase


class ConfigSQLite(ConfigPynencBase):
    """
    Configuration for SQLite-based components.

    Configuration Fields:
    :cvar ConfigField[str] sqlite_db_path:
        Path to the SQLite database file. An empty string indicates
        that a default temporary path should be used.
    """

    sqlite_db_path = ConfigField("")
