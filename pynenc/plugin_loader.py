"""
Plugin loader for Pynenc.

This module handles automatic discovery and loading of plugins registered via entry points.
It ensures that all plugin classes (e.g., brokers, state backends, orchestrators) are imported
into memory, making them available for subclass discovery even when not using the builder.
"""

import importlib.metadata
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


def load_all_plugins() -> None:
    """
    Discover and load all plugins registered under the 'pynenc.plugins' entry point group.

    This function iterates through all entry points in the 'pynenc.plugins' group,
    imports the specified modules, and ensures plugin classes are available for
    subclass discovery. This is called at Pynenc app startup to enable dynamic
    component resolution.

    :raises ImportError: If a plugin module cannot be imported
    """
    eps = importlib.metadata.entry_points(group="pynenc.plugins")
    for ep in eps:
        try:
            # Import the module to register its classes
            __import__(ep.module)
        except ImportError as e:
            logger.warning(f"Failed to load plugin {ep.name} from {ep.module}: {e}")
