"""
Pynmon - Web monitoring interface for Pynenc.

Python Compatibility:
- Requires Python <3.13 due to FastAPI/Pydantic v2 dependencies
- Core pynenc supports Python 3.11+

This module provides a web-based monitoring UI for inspecting and managing
Pynenc task orchestration in real-time.
"""

from pynmon.app import get_pynenc_instance, start_monitor

__all__ = ["start_monitor", "get_pynenc_instance"]
