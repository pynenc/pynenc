import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..app import Pynenc


@dataclass
class PynencCLINamespace(argparse.Namespace):
    app: str | None = None
    verbose: bool | None = None
    app_instance: Optional["Pynenc"] = None
