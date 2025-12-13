"""
View helper utilities for pynmon.

Common patterns for view functions: error handling, argument formatting, etc.
"""

import json
import logging
import traceback
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.responses import HTMLResponse

if TYPE_CHECKING:
    from fastapi.templating import Jinja2Templates
    from pynenc.call import Call

logger = logging.getLogger("pynmon.util.view_helpers")


def render_error_response(
    templates: "Jinja2Templates",
    request: Request,
    title: str,
    message: str,
    status_code: int = 500,
) -> HTMLResponse:
    """
    Render a standardized error response.

    :param templates: Jinja2Templates instance
    :param request: FastAPI request
    :param title: Error title
    :param message: Error message
    :param status_code: HTTP status code
    :return: HTMLResponse with error template
    """
    return templates.TemplateResponse(
        "shared/error.html",
        {
            "request": request,
            "title": title,
            "message": message,
        },
        status_code=status_code,
    )


def format_call_arguments(call: "Call", max_length: int = 500) -> dict[str, str]:
    """
    Format call arguments for template display.

    :param call: The call object
    :param max_length: Maximum string length before truncation
    :return: Dictionary of formatted argument strings
    """
    try:
        formatted = {}
        for key, value in call.arguments.kwargs.items():
            str_value = str(value)
            if len(str_value) > max_length:
                formatted[key] = f"{str_value[:max_length]}... (truncated)"
            else:
                formatted[key] = str_value
        return formatted
    except Exception as e:
        logger.error(f"Error formatting arguments: {e}")
        return {"Error": f"Could not format arguments: {e}"}


def format_serialized_arguments(
    serialized_args: dict | str | None,
) -> tuple[dict[str, str], dict | None]:
    """
    Format serialized arguments for display.

    :param serialized_args: Serialized arguments (dict or JSON string)
    :return: Tuple of (formatted_args, raw_args)
    """
    if serialized_args is None:
        return {}, None

    try:
        raw_args = _parse_raw_args(serialized_args)
        formatted = _format_args_dict(raw_args)
        return formatted, raw_args
    except (json.JSONDecodeError, TypeError, AttributeError):
        return {"raw": str(serialized_args)}, None


def _parse_raw_args(serialized_args: dict | str) -> dict:
    """Parse serialized arguments to dict."""
    if isinstance(serialized_args, dict):
        return serialized_args
    return json.loads(serialized_args)


def _format_args_dict(args: dict) -> dict[str, str]:
    """Format argument dictionary for display."""
    formatted = {}
    for key, value in args.items():
        if isinstance(value, dict | list):
            formatted[key] = json.dumps(value, indent=2)
        else:
            formatted[key] = str(value)
    return formatted


def log_exception(logger: logging.Logger, message: str, exc: Exception) -> None:
    """Log an exception with full traceback."""
    logger.error(f"{message}: {exc}")
    logger.error(traceback.format_exc())
