Configuration System
====================

Pynenc's configuration system is designed for high flexibility and modularity, supporting a variety of sources and formats for configuration data. This system is particularly suited for distributed systems where configuration might vary across different environments or components.

Configuration Sources
---------------------

The configuration values can be determined from various sources, with the following priority (from highest to lowest):

1. Direct assignment in the config instance (not recommended)
2. Environment variables
3. Configuration file path specified by environment variables
4. Configuration file path (YAML, TOML, JSON)
5. `pyproject.toml`
6. Default values specified in the `ConfigField`

Hierarchical Configuration
--------------------------

Pynenc supports a hierarchical configuration system, allowing for nested configuration classes. This feature enables specifying configuration at different levels, from general to specific.

Example:
.. code-block:: python

   class ConfigGrandpa(ConfigBase):
       test_field = ConfigField("grandpa_value")

   class ConfigParent(ConfigGrandpa):
       test_field = ConfigField("parent_value")

   class ConfigChild(ConfigParent):
       test_field = ConfigField("child_value")

In `pyproject.toml`, configurations can be specified at different levels:

.. code-block:: toml

   [tool.pynenc]
   test_field = "toml_value"

   [tool.pynenc.grandpa]
   test_field = "toml_grandpa_value"

   [tool.pynenc.parent]
   test_field = "toml_parent_value"

   [tool.pynenc.child]
   test_field = "toml_child_value"

The most specific (child) configuration will take precedence over more general (parent/grandpa) configurations and the default.

Environment Variables
---------------------

Environment variables can be used to override configuration values. They follow two naming conventions:

1. `PYNENC__<CONFIG_CLASS_NAME>__<FIELD_NAME>` for setting values specific to a configuration class.
2. `PYNENC__<FIELD_NAME>` for default values that apply across all configuration classes.

Example:
.. code-block:: shell

   # Specific to a configuration class
   export PYNENC__CONFIGCHILD__TEST_FIELD="env_child_value"

   # Default value for all configuration classes
   export PYNENC__TEST_FIELD="env_default_value"

In the first example, `test_field` in `ConfigChild` is overridden with "env_child_value". In the second example, `test_field` is set to "env_default_value" for any configuration class that does not have a more specific value defined.

Type Casting in ConfigField
---------------------------

`ConfigField` ensures that the type of the configuration value is preserved. Values from files or environment variables are cast to the specified type, and an exception is raised if casting is not possible.

Specifying Configuration File Path
----------------------------------

A specific configuration file can be indicated using the `PYNENC__FILEPATH` environment variable. Additionally, a file exclusive to a particular `ConfigBase` instance can be specified, e.g., `PYNENC__TESTCONFIG__FILEPATH` for `TestConfig`.

.. note::
   The configuration system is designed to be easily extendable, allowing users to create custom configuration classes that inherit from `ConfigBase`. This flexibility facilitates the modification of specific parts of the configuration as necessary for each system.

Multi-Inheritance Support
-------------------------

Pynenc's configuration system supports multiple inheritance, allowing for the combination of configurations from different parent classes. This feature is particularly useful when different components of the system share common configuration options.

Example:
.. code-block:: python

   class ConfigOrchestrator(ConfigBase):
       ...

   class ConfigOrchestratorRedis(ConfigOrchestrator, ConfigRedis):
       ...

In this example, `ConfigOrchestratorRedis` combines the default configurations of both `ConfigOrchestrator` and `ConfigRedis`.

Extending Configuration
-----------------------

Users can extend the configuration system by creating custom configuration classes that inherit from `ConfigBase`. This flexibility allows for the easy modification of specific parts of the configuration as necessary for each system.

.. note::
   The configuration system ensures that the same configuration field is not defined in multiple parent classes, preventing conflicts and ensuring deterministic behavior.
