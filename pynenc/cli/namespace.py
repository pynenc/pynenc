import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.app import Pynenc


@dataclass
class PynencCLINamespace(argparse.Namespace):
    """
    A dataclass for holding command line arguments in the Pynenc CLI.

    This class is a custom namespace for parsing command line arguments using `argparse`.
    It extends the functionality of `argparse.Namespace` by explicitly defining fields for
    expected arguments. This approach enhances code clarity and type-checking.

    :cvar str | None app:
        The module and name of the application. Default is None.
    :cvar bool | None verbose:
        Flag to increase output verbosity. Default is None.
    :cvar Pynenc | None app_instance:
        An instance of the Pynenc application, set after parsing arguments.
    """

    app: str | None = None
    verbose: bool | None = None
    app_instance: "Pynenc | None" = None
