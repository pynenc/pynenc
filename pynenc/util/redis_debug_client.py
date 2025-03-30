"""
Redis Debug Client

This module provides instrumentation for Redis commands to track performance
and identify bottlenecks. It captures detailed call stacks, timing information,
and aggregates statistics to help diagnose Redis performance issues.
"""

# mypy: ignore-errors
# Ignore mypy type checking for this file due to complex interactions with Redis internals


import inspect
import logging
import threading
import time
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, NamedTuple, Optional, TypeVar

import redis

from pynenc.conf.config_redis import ConfigRedis
from pynenc.util.redis_client import _POOLS_LOCK
from pynenc.util.redis_client import get_redis_client as original_get_redis_client

logger = logging.getLogger(__name__)

# Define a type variable for function return types
T = TypeVar("T")

# Configuration constants
DEFAULT_STACK_DEPTH = 7  # Show up to 7 frames in stack traces
MAX_CALL_STACKS_PER_CALLER = 5  # Store up to 5 different call paths per caller
MAX_TOP_CALLERS = 10  # Show top 10 callers in the report
SLOW_COMMAND_THRESHOLD = 0.1  # Log commands taking > 0.1s at INFO level
MAX_ARG_LENGTH = 100  # Truncate arguments longer than this


# =============================================================================
# Data models for tracking Redis command performance
# =============================================================================


class CommandStats(NamedTuple):
    """Statistics for a Redis command."""

    count: int  # Number of times command was executed
    total_time: float  # Total execution time in seconds
    avg_time: float  # Average execution time in seconds
    max_time: float  # Maximum execution time in seconds


class CallStack(NamedTuple):
    """Information about a call stack."""

    frames: list[str]  # The call stack frames
    count: int = 1  # How many times this call stack has been observed


class CallerInfo(NamedTuple):
    """Information about a Redis command caller."""

    module_name: str
    class_name: Optional[str]
    function_name: str
    line_number: int

    @property
    def formatted_caller(self) -> str:
        """Format the caller information as a string."""
        if self.class_name:
            return f"{self.module_name}.{self.class_name}.{self.function_name}:{self.line_number}"
        return f"{self.module_name}.{self.function_name}:{self.line_number}"


# =============================================================================
# Call stack collection utilities
# =============================================================================


def get_frame_info(frame: inspect.FrameInfo) -> CallerInfo:
    """
    Extract caller information from a frame.

    :param frame: Stack frame to analyze
    :return: Structured caller information
    """
    module_name = frame.f_globals.get("__name__", "")
    lineno = frame.f_lineno
    function_name = frame.f_code.co_name

    # Check if frame has self (class method)
    class_name = None
    if "self" in frame.f_locals:
        class_name = frame.f_locals["self"].__class__.__name__

    return CallerInfo(
        module_name=module_name,
        class_name=class_name,
        function_name=function_name,
        line_number=lineno,
    )


def collect_call_stack() -> tuple[str, list[str]]:
    """
    Collect call stack information from the current execution context.

    :return: A tuple containing (primary_caller, call_stack)
    """
    frame = inspect.currentframe()
    caller = "unknown"
    call_stack = []

    try:
        if frame:
            # Go back in the call stack to find the non-Redis caller
            current_frame = frame.f_back
            frame_list = []

            # Collect all frames in the stack
            while current_frame:
                module_name = current_frame.f_globals.get("__name__", "")

                # Skip Redis and debug client frames
                if not module_name.startswith("redis") and not module_name.endswith(
                    "redis_debug_client"
                ):
                    caller_info = get_frame_info(current_frame)
                    frame_info = caller_info.formatted_caller

                    # Add to our call stack
                    frame_list.append(frame_info)

                    # First non-Redis caller becomes our primary caller
                    if caller == "unknown":
                        caller = frame_info

                current_frame = current_frame.f_back

            # Create a more specific caller ID for better resolution
            if len(frame_list) >= 2:
                # Use main caller + immediate parent
                # This helps distinguish different call paths to the same function
                caller = f"{frame_list[0]}←{frame_list[1].split('.')[-1]}"
            else:
                caller = frame_list[0] if frame_list else "unknown"

            # Reverse to get chronological order (deepest calls first)
            call_stack = list(reversed(frame_list))
    finally:
        if frame:
            del frame  # Avoid reference cycles

    return caller, call_stack


# =============================================================================
# Formatting utilities
# =============================================================================


def format_arg(arg: Any) -> str:
    """
    Format a single argument for logging, truncating if too long.

    :param arg: Argument to format
    :return: String representation of the argument
    """
    str_arg = str(arg)
    if len(str_arg) > MAX_ARG_LENGTH:
        return f"{str_arg[:MAX_ARG_LENGTH]}... [truncated, len={len(str_arg)}]"
    return str_arg


def format_caller_for_display(caller: str, width: int) -> str:
    """
    Format a caller string for display, with clean truncation for better alignment.

    :param caller: The caller string to format
    :param width: Maximum width for display
    :return: Formatted caller string with consistent width
    """
    # If it fits, return as is with padding
    if len(caller) <= width:
        return caller.ljust(width)

    # Use a fixed format for truncation to ensure consistent alignment
    return format_truncated_caller(caller, width)


def format_truncated_caller(caller: str, width: int) -> str:
    """
    Format a long caller string with clean truncation.

    Uses a consistent pattern to ensure table alignment:
    - For long paths: keeps start and end with ... in middle
    - Guarantees exact width output

    :param caller: Caller identifier to format
    :param width: Exact width to format to
    :return: Formatted caller string with exact width
    """
    # Extract line number if present (for keeping at the end)
    line_number = ""
    if ":" in caller:
        parts = caller.rsplit(":", 1)
        if len(parts) == 2 and parts[1].isdigit():
            caller = parts[0]
            line_number = ":" + parts[1]

    # Calculate available space for the caller path
    path_width = width - len(line_number)

    # If we can't even fit a minimal truncated format, use a fixed pattern
    if path_width < 10:  # Need at least room for "...X..."
        return ("..." + caller[-width + 3 :]).ljust(width)

    # For normal truncation, keep meaningful parts of the path

    # Keep the method/function name at the end
    if "." in caller:
        path_parts = caller.split(".")

        # Extract the end part (usually class.method or module.function)
        if len(path_parts) >= 2:
            end_part = path_parts[-1]

            # If class method pattern detected, include class name
            if len(path_parts) >= 3:
                end_part = f"{path_parts[-2]}.{end_part}"
        else:
            end_part = path_parts[0]

        # Calculate how much space we have for the start part
        # Formula: width - (length of end part + ellipsis + line number)
        start_length = path_width - len(end_part) - 3  # 3 for "..."

        if start_length > 0:
            # Keep start of the path and truncate the middle
            start_part = caller[:start_length]
            return f"{start_part}...{end_part}{line_number}".ljust(width)
        else:
            # Not enough space for the start, truncate the end part
            return f"...{end_part[-path_width+3:]}{line_number}".ljust(width)

    # Simple case: no dots in the path
    if len(caller) > path_width:
        return f"...{caller[-path_width+3:]}{line_number}".ljust(width)

    return f"{caller}{line_number}".ljust(width)


def _format_long_caller(caller_parts: list[str], width: int) -> str:
    """Format a long caller with many path components."""
    # For module path, keep first letter of each early segment
    module_prefix = ".".join(part[0] for part in caller_parts[:-3])
    # Keep the last 3 parts intact (class.method:line)
    important_suffix = ".".join(caller_parts[-3:])

    # Combine with a dot
    abbreviated = f"{module_prefix}.{important_suffix}"

    # If still too long, truncate the middle
    if len(abbreviated) > width:
        # Keep the most important part (usually class.method:line)
        important_part = ".".join(caller_parts[-2:])
        # Calculate space available for the third-last component
        avail_space = width - len(important_part) - 5  # 5 = len("...") + len(".")

        # Use as much of the third-last component as fits
        prefix = caller_parts[-3][:avail_space] if avail_space > 0 else ""
        return f"{prefix}...{important_part}".ljust(width)

    return abbreviated.ljust(width)


def _format_short_caller(caller: str, caller_parts: list[str], width: int) -> str:
    """Format a shorter caller with fewer path components."""
    # For shorter paths, keep the end and use ellipsis
    end_part = ".".join(caller_parts[-2:]) if len(caller_parts) >= 2 else caller

    # If even the end part is too long, truncate it
    if len(end_part) > width - 4:  # 4 = len("...")
        return f"...{end_part[-(width-4):]}".ljust(width)

    # Otherwise keep as much of the start as possible
    prefix_len = width - len(end_part) - 4
    prefix = caller[:prefix_len] if prefix_len > 0 else ""
    return f"{prefix}...{end_part}".ljust(width)


# =============================================================================
# Core statistics tracking class
# =============================================================================


class RedisStats:
    """Tracks Redis command statistics with call stack information."""

    def __init__(self) -> None:
        """Initialize Redis stats tracking."""
        # Main stats structure: caller -> command -> stats
        self.stats: dict[str, dict[str, CommandStats]] = defaultdict(
            lambda: defaultdict(lambda: CommandStats(0, 0.0, 0.0, 0.0))
        )
        # Store multiple call stacks for each caller
        self.call_stacks: dict[str, list[CallStack]] = defaultdict(list)
        self.lock = threading.Lock()
        self.enabled = False

    def start_tracking(self) -> None:
        """Start tracking Redis command statistics."""
        with self.lock:
            self.stats.clear()
            self.call_stacks.clear()
            self.enabled = True

    def stop_tracking(self) -> None:
        """Stop tracking Redis command statistics."""
        with self.lock:
            self.enabled = False

    def add_stat(
        self, caller: str, call_stack: list[str], command: str, execution_time: float
    ) -> None:
        """
        Add a command execution stat with call stack information.

        :param caller: The code location that called the Redis command
        :param call_stack: The call frames leading to this command
        :param command: The Redis command name
        :param execution_time: How long the command took to execute
        """
        if not self.enabled:
            return

        with self.lock:
            # Update command statistics
            self._update_command_stats(caller, command, execution_time)

            # Track call stack information
            if call_stack:
                self._update_call_stack(caller, call_stack)

    def _update_command_stats(
        self, caller: str, command: str, execution_time: float
    ) -> None:
        """Update statistics for a specific command."""
        current = self.stats[caller][command]
        new_count = current.count + 1
        new_total = current.total_time + execution_time
        new_avg = new_total / new_count
        new_max = max(current.max_time, execution_time)

        self.stats[caller][command] = CommandStats(
            count=new_count,
            total_time=new_total,
            avg_time=new_avg,
            max_time=new_max,
        )

    def _update_call_stack(self, caller: str, call_stack: list[str]) -> None:
        """Update call stack information for a caller."""
        # Convert to tuple for hashability
        stack_tuple = tuple(call_stack)

        # Check if we've seen this exact stack before
        found = False
        for i, cs in enumerate(self.call_stacks[caller]):
            if tuple(cs.frames) == stack_tuple:
                # Update frequency count
                self.call_stacks[caller][i] = CallStack(
                    frames=cs.frames, count=cs.count + 1
                )
                found = True
                break

        # If this is a new stack, add it
        if not found:
            # Limit to max call stacks per caller to avoid memory bloat
            if len(self.call_stacks[caller]) < MAX_CALL_STACKS_PER_CALLER:
                self.call_stacks[caller].append(CallStack(frames=call_stack))

    # -------------------------------------------------------------------------
    # Report generation methods
    # -------------------------------------------------------------------------

    def get_sorted_commands(self) -> list[tuple[str, str, CommandStats]]:
        """Get commands sorted by total time (for programmatic use)."""
        with self.lock:
            return self._get_sorted_commands_list()

    def get_report(
        self,
        top_n: int = 20,
        stack_depth: int = DEFAULT_STACK_DEPTH,
        table_width: int = 140,
    ) -> str:
        """
        Generate a performance report for Redis commands.

        :param top_n: Number of top commands to include
        :param stack_depth: Number of frames to show in call stacks
        :param table_width: Maximum width for the table
        :return: Formatted report as a string
        """
        with self.lock:
            if not self.stats:
                return "No Redis statistics collected."

            # Collect the report parts
            report_parts = []

            # Add header
            report_parts.extend(
                ["Redis Commands Performance Report", "=" * min(80, table_width)]
            )

            # Get the sorted command list
            all_commands = self._get_sorted_commands_list()

            # Add the commands table
            report_parts.extend(
                self._format_commands_table(all_commands, top_n, table_width)
            )

            # Add summary statistics
            report_parts.extend(self._get_summary_stats(all_commands))

            # Add top callers section
            report_parts.extend(
                self._get_top_callers_section(all_commands, stack_depth)
            )

            # Join all parts into a single report
            return "\n".join(report_parts)

    def _get_sorted_commands_list(self) -> list[tuple[str, str, CommandStats]]:
        """Get commands sorted by total execution time."""
        all_commands = []
        for caller, commands in self.stats.items():
            for cmd, stats in commands.items():
                all_commands.append((caller, cmd, stats))

        # Sort by total time (most expensive first)
        all_commands.sort(key=lambda x: x[2].total_time, reverse=True)
        return all_commands

    def _get_summary_stats(
        self, all_commands: list[tuple[str, str, CommandStats]]
    ) -> list[str]:
        """Generate the summary statistics section."""
        if not all_commands:
            return []

        total_commands = sum(stats.count for _, _, stats in all_commands)
        total_time = sum(stats.total_time for _, _, stats in all_commands)
        avg_time = (total_time / total_commands) * 1000 if total_commands > 0 else 0

        return [
            f"Total Commands: {total_commands}",
            f"Total Time: {total_time:.3f}s",
            f"Average Time per Command: {avg_time:.2f}ms"
            if total_commands > 0
            else "No commands executed.",
        ]

    # Update _get_top_callers_section method to better display call stacks
    def _get_top_callers_section(
        self, all_commands: list[tuple[str, str, CommandStats]], stack_depth: int
    ) -> list[str]:
        """Generate the top callers section with call stacks and improved formatting."""
        if not all_commands:
            return []

        report = ["\nTop Callers by Total Time:"]

        # Aggregate stats by caller
        caller_stats = self._aggregate_stats_by_caller(all_commands)

        # Sort and display top callers
        sorted_callers = sorted(
            caller_stats.items(), key=lambda x: x[1].total_time, reverse=True
        )[:MAX_TOP_CALLERS]

        for caller, stats in sorted_callers:
            # Basic stats line
            report.append(
                f"{caller}: {stats.count} commands, {stats.total_time:.3f}s total, {stats.avg_time*1000:.2f}ms avg"
            )

            # Add call stacks if available
            if caller in self.call_stacks and self.call_stacks[caller]:
                # Collect all unique frames across paths for comparison
                all_frames = set()
                for call_stack in self.call_stacks[caller]:
                    all_frames.update(call_stack.frames)

                # Sort stacks by frequency
                sorted_stacks = sorted(
                    self.call_stacks[caller], key=lambda cs: cs.count, reverse=True
                )

                # Track already shown frames to detect duplicates
                shown_paths = set()

                # Show up to 3 most common call stacks
                for idx, call_stack in enumerate(sorted_stacks[:3]):
                    # Convert frame list to tuple for hashability
                    frames_tuple = tuple(call_stack.frames)

                    # Skip this stack if it's essentially the same as one we've shown
                    # (This handles small variations in line numbers but same call path)
                    if self._is_duplicate_path(frames_tuple, shown_paths):
                        continue

                    # Different label for first vs alternate paths
                    if idx == 0:
                        report.append(
                            f"  Most common call path (seen {call_stack.count} times):"
                        )
                    else:
                        report.append(
                            f"  Alternate call path (seen {call_stack.count} times):"
                        )

                    # Show the most recent frames in the call stack (up to stack_depth)
                    frames_to_show = min(stack_depth, len(call_stack.frames))
                    for frame in call_stack.frames[-frames_to_show:]:
                        # Format frame for readability
                        frame_display = self._format_frame_for_display(frame)
                        report.append(f"    - {frame_display}")

                    # Add this path to shown paths
                    shown_paths.add(frames_tuple)

        return report

    def _is_duplicate_path(
        self, new_path: tuple[str, ...], existing_paths: set[tuple[str, ...]]
    ) -> bool:
        """
        Check if a call path is essentially a duplicate of an existing path.

        This compares the function names and classes, ignoring minor line number differences.

        :param new_path: The new call path to check
        :param existing_paths: Previously shown call paths
        :return: True if this path is essentially a duplicate
        """
        if not existing_paths:
            return False

        # For each existing path, check if it matches the new path
        for existing_path in existing_paths:
            # Must be same length to be considered a potential match
            if len(new_path) != len(existing_path):
                continue

            # Check if they're essentially the same path
            matches = 0
            for i, (new_frame, old_frame) in enumerate(zip(new_path, existing_path)):
                # Strip line numbers for comparison
                new_parts = (
                    new_frame.split(":", 1)[0] if ":" in new_frame else new_frame
                )
                old_parts = (
                    old_frame.split(":", 1)[0] if ":" in old_frame else old_frame
                )

                # If same module + function, count as match
                if new_parts == old_parts:
                    matches += 1
                # Small variations in line numbers are ok for most frames
                # except the immediate caller (last frame)
                elif (
                    i < len(new_path) - 1
                    and new_parts.split(".")[-1] == old_parts.split(".")[-1]
                ):
                    matches += 0.8  # Partial match

            # If > 90% same, consider it a duplicate
            if matches / len(new_path) > 0.9:
                return True

        return False

    def _format_frame_for_display(self, frame: str) -> str:
        """
        Format a stack frame for display.

        Highlights important parts of the frame and improves readability.

        :param frame: Raw stack frame string
        :return: Formatted frame for display
        """
        # If it's a long path, improve readability
        if len(frame) > 80:
            # Split on module/class/method
            if "." in frame:
                parts = frame.split(".")

                # Extract line number if present
                line_number = ""
                if ":" in parts[-1]:
                    method_parts = parts[-1].split(":")
                    parts[-1] = method_parts[0]
                    line_number = ":" + method_parts[1]

                # For long module paths, abbreviate
                if len(parts) > 3:
                    # Keep the module root
                    module_root = parts[0]
                    # Abbreviate middle modules
                    middle_parts = ".".join(p[0] for p in parts[1:-2])
                    # Keep class and method
                    class_name = parts[-2]
                    method_name = parts[-1]

                    # Combine with better visual structure
                    return f"{module_root}.{middle_parts}.{class_name}.{method_name}{line_number}"

            # If no special formatting applies, return as is
            return frame

        return frame

    # Update the _format_commands_table method for better alignment

    def _format_commands_table(
        self,
        all_commands: list[tuple[str, str, CommandStats]],
        top_n: int,
        table_width: int = 140,
    ) -> list[str]:
        """Format the commands table section of the report with improved alignment."""
        if not all_commands:
            return ["No commands recorded."]

        report = []
        report.append(f"Top {min(top_n, len(all_commands))} most expensive commands:")
        report.append("Caller format: module.class.method:line_number")

        # Calculate available width for caller column based on total width
        # Reserve at least 20 chars for command and 30 for numeric columns
        numeric_width = 30  # 10 each for count, total, avg
        reserved_width = numeric_width + 20  # command + numerics

        # Calculate remaining width for caller column, ensuring at least 30 chars
        caller_width = max(min(table_width - reserved_width, 60), 30)
        cmd_width = max(min(20, table_width - caller_width - numeric_width), 10)

        # Calculate space for numeric columns
        remaining_width = table_width - caller_width - cmd_width
        count_width = max(8, min(10, remaining_width // 4))
        total_width = max(8, min(10, remaining_width // 4))
        avg_width = max(8, min(10, remaining_width // 4))
        max_width = max(8, min(10, remaining_width // 4))

        # Create the header row
        header = (
            f"{'Caller':<{caller_width}} "
            f"{'Command':<{cmd_width}} "
            f"{'Count':>{count_width}} "
            f"{'Total(s)':>{total_width}} "
            f"{'Avg(ms)':>{avg_width}} "
            f"{'Max(ms)':>{max_width}}"
        )
        report.append(header)

        # Create separator line
        separator = "-" * min(len(header), table_width)
        report.append(separator)

        # Add command rows
        for caller, cmd, stats in all_commands[:top_n]:
            # Format caller with clean truncation
            caller_display = format_caller_for_display(caller, caller_width)

            # Ensure command is cleanly truncated
            if len(cmd) > cmd_width:
                cmd_display = cmd[: cmd_width - 3] + "..."
            else:
                cmd_display = cmd.ljust(cmd_width)

            # Format values with consistent width
            count_display = f"{stats.count:>{count_width}d}"
            total_display = f"{stats.total_time:>{total_width}.3f}"
            avg_display = f"{stats.avg_time*1000:>{avg_width}.2f}"
            max_display = f"{stats.max_time*1000:>{max_width}.2f}"

            # Combine into the row
            report.append(
                f"{caller_display} "
                f"{cmd_display} "
                f"{count_display} "
                f"{total_display} "
                f"{avg_display} "
                f"{max_display}"
            )

        # Add footer line
        report.append(separator)

        return report

    def _aggregate_stats_by_caller(
        self, all_commands: list[tuple[str, str, CommandStats]]
    ) -> dict[str, CommandStats]:
        """Aggregate command statistics by caller."""
        caller_stats = {}

        # First pass: sum up stats for each caller
        for caller, _, stats in all_commands:
            if caller not in caller_stats:
                caller_stats[caller] = stats
            else:
                current = caller_stats[caller]
                caller_stats[caller] = CommandStats(
                    current.count + stats.count,
                    current.total_time + stats.total_time,
                    0,  # Will calculate average later
                    max(current.max_time, stats.max_time),
                )

        # Second pass: calculate averages
        for caller, stats in caller_stats.items():
            if stats.count > 0:
                caller_stats[caller] = CommandStats(
                    stats.count,
                    stats.total_time,
                    stats.total_time / stats.count,
                    stats.max_time,
                )

        return caller_stats


# =============================================================================
# Command logging and Redis client wrapper
# =============================================================================


def log_redis_command(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to log Redis commands with timing and call stack information.

    :param func: Redis command function to wrap
    :return: Wrapped function with logging and stats collection
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        # Get function name for logging
        func_name = func.__name__

        # Format arguments for logging
        formatted_args = [format_arg(arg) for arg in args[1:]]  # Skip self
        formatted_kwargs = {k: format_arg(v) for k, v in kwargs.items()}

        # Log command start with DEBUG level
        log_prefix = "REDIS_CMD"
        logger.debug(
            f"{log_prefix} START: {func_name}({', '.join(formatted_args)}, {formatted_kwargs})"
        )

        # Get the caller information and stack trace
        caller, call_stack = collect_call_stack()

        # Execute command and time it
        start_time = time.time()
        try:
            # Execute the Redis command
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time

            # Add to statistics
            redis_stats.add_stat(caller, call_stack, func_name, elapsed)

            # Format result for logging (with truncation for long results)
            result_str = format_arg(result)

            # Log command completion with appropriate level based on performance
            if elapsed > SLOW_COMMAND_THRESHOLD:
                # Use INFO level for slow commands
                logger.info(
                    f"{log_prefix} SLOW: {func_name} took {elapsed:.6f}s - "
                    f"Result: {result_str} - Called by: {caller}"
                )
            else:
                # Use DEBUG level for normal commands
                logger.debug(
                    f"{log_prefix} END: {func_name} took {elapsed:.6f}s - Result: {result_str}"
                )

            return result

        except Exception as e:
            # Handle and log exceptions
            elapsed = time.time() - start_time

            # Still record failed commands in stats
            redis_stats.add_stat(caller, call_stack, f"{func_name}_ERROR", elapsed)

            # Log the error with ERROR level
            logger.error(
                f"{log_prefix} ERROR: {func_name} failed after {elapsed:.6f}s - "
                f"{type(e).__name__}: {e} - Called by: {caller}"
            )
            # Re-raise the original exception
            raise

    return wrapper


class DebugRedisClient(redis.Redis):
    """Redis client that logs all commands with timing information."""

    def __getattribute__(self, name: str) -> Any:
        """
        Intercept attribute access to wrap Redis commands with logging.

        :param name: Attribute name
        :return: Wrapped command or original attribute
        """
        attr = super().__getattribute__(name)

        # Only wrap callable Redis commands, not internal methods or attributes
        if (
            callable(attr)
            and not name.startswith("_")
            and name
            not in [
                "execute_command",
                "parse_response",
                "connection_pool",
                "from_url",
            ]
        ):
            return log_redis_command(attr)
        return attr

    def _execute_command(self, *args: Any, **options: Any) -> Any:
        """
        Override the core Redis client execute_command method to track all operations.

        This is the central method that all Redis commands ultimately call.
        """
        # Get command name
        command_name = args[0] if args else "unknown"

        # Get the caller information and stack trace
        caller, call_stack = collect_call_stack()

        # Format arguments for logging (with some limitations)
        formatted_args = [format_arg(arg) for arg in args[1:]]
        formatted_kwargs = {
            k: format_arg(v) for k, v in options.items() if k not in ["keys"]
        }

        log_prefix = "REDIS_CMD"
        logger.debug(
            f"{log_prefix} START: {command_name}({', '.join(formatted_args)}, {formatted_kwargs})"
        )

        # Execute command and time it
        start_time = time.time()
        try:
            # Call the original Redis client method
            result = super()._execute_command(*args, **options)
            elapsed = time.time() - start_time

            # Add to statistics
            redis_stats.add_stat(caller, call_stack, command_name, elapsed)

            # Format result for logging
            result_str = format_arg(result)

            # Log command completion
            if elapsed > SLOW_COMMAND_THRESHOLD:
                logger.info(
                    f"{log_prefix} SLOW: {command_name} took {elapsed:.6f}s - "
                    f"Result: {result_str} - Called by: {caller}"
                )
            else:
                logger.debug(
                    f"{log_prefix} END: {command_name} took {elapsed:.6f}s - Result: {result_str}"
                )

            return result

        except Exception as e:
            # Handle and log exceptions
            elapsed = time.time() - start_time

            # Still record failed commands in stats
            redis_stats.add_stat(caller, call_stack, f"{command_name}_ERROR", elapsed)

            # Log the error
            logger.error(
                f"{log_prefix} ERROR: {command_name} failed after {elapsed:.6f}s - "
                f"{type(e).__name__}: {e} - Called by: {caller}"
            )
            # Re-raise the original exception
            raise


# =============================================================================
# Patcher for redis_client function
# =============================================================================


def patch_redis_client() -> None:
    """
    Patch the get_redis_client function to return a DebugRedisClient instance.
    This ensures all Redis operations throughout the application are monitored.
    """
    import pynenc.util.redis_client

    @wraps(original_get_redis_client)
    def patched_get_redis_client(conf: ConfigRedis) -> redis.Redis:
        """
        Patched version of get_redis_client that returns a DebugRedisClient.
        Uses the same connection pooling logic as the original function.
        """
        global _REDIS_POOLS

        if conf.redis_url:
            pool_key = conf.redis_url
        else:
            pool_key = f"{conf.redis_host}:{conf.redis_port}:{conf.redis_db}:{conf.redis_username or ''}:{conf.redis_password or ''}"

        with _POOLS_LOCK:
            if pool_key not in _REDIS_POOLS:
                max_connections = conf.redis_pool_max_connections
                logger.info(
                    f"REDIS_DEBUG: Creating new pool with max_connections={max_connections} "
                    f"for {pool_key}"
                )

                if conf.redis_url:
                    _REDIS_POOLS[pool_key] = redis.ConnectionPool.from_url(
                        conf.redis_url,
                        max_connections=max_connections,
                        socket_timeout=conf.socket_timeout,
                        socket_connect_timeout=conf.socket_connect_timeout,
                        socket_keepalive=True,
                        health_check_interval=conf.redis_pool_health_check_interval,
                        retry_on_timeout=True,
                    )
                else:
                    _REDIS_POOLS[pool_key] = redis.ConnectionPool(
                        host=conf.redis_host,
                        port=conf.redis_port,
                        db=conf.redis_db,
                        username=None
                        if not conf.redis_username
                        else conf.redis_username,
                        password=None
                        if not conf.redis_password
                        else conf.redis_password,
                        socket_timeout=conf.socket_timeout,
                        socket_connect_timeout=conf.socket_connect_timeout,
                        socket_keepalive=True,
                        health_check_interval=conf.redis_pool_health_check_interval,
                        max_connections=max_connections,
                        retry_on_timeout=True,
                    )

        # Create a debug client with the same connection pool
        client = DebugRedisClient(connection_pool=_REDIS_POOLS[pool_key])

        # Log a message about the new debug client
        connection_info = (
            f"{conf.redis_host}:{conf.redis_port}/{conf.redis_db}"
            if not conf.redis_url
            else conf.redis_url
        )
        logger.info(
            f"REDIS_DEBUG: Created debug client with connection pooling for {connection_info}"
        )

        # Test the connection with a ping
        try:
            ping_start = time.time()
            res = client.ping()
            ping_elapsed = time.time() - ping_start
            logger.info(
                f"REDIS_DEBUG: Redis ping response: {res} in {ping_elapsed:.6f}s"
            )
        except Exception as e:
            logger.error(f"REDIS_DEBUG: Redis ping failed: {e}")

        return client

    # Replace the original function with our patched version
    pynenc.util.redis_client.get_redis_client = patched_get_redis_client
    logger.info("REDIS_DEBUG: get_redis_client patched to use DebugRedisClient")


# =============================================================================
# Pool Monitoring Functions
# =============================================================================


def log_pool_stats() -> None:
    """Log statistics about all Redis connection pools."""
    global _REDIS_POOLS

    for pool_key, pool in _REDIS_POOLS.items():
        if hasattr(pool, "_available_connections") and hasattr(
            pool, "_in_use_connections"
        ):
            available = len(pool._available_connections)
            in_use = len(pool._in_use_connections)
            max_conn = pool.max_connections

            usage_pct = (in_use / max_conn) * 100 if max_conn else 0

            logger.debug(
                f"REDIS_DEBUG: Pool {pool_key}: "
                f"{in_use} in use, {available} available, "
                f"{in_use + available}/{max_conn} total ({usage_pct:.1f}%)"
            )

            # Alert on high usage
            if usage_pct > 80:
                logger.warning(
                    f"REDIS_DEBUG: Pool {pool_key} at {usage_pct:.1f}% capacity! "
                    f"Consider increasing max_connections (currently {max_conn})"
                )


def monitor_redis_pools(interval_seconds: int = 60) -> None:
    """
    Start a background thread to monitor Redis connection pools.

    :param interval_seconds: How often to log pool statistics
    """

    def _monitor_loop() -> None:
        while True:
            log_pool_stats()
            time.sleep(interval_seconds)

    monitor_thread = threading.Thread(
        target=_monitor_loop, name="RedisPoolMonitor", daemon=True
    )
    monitor_thread.start()
    logger.info("REDIS_DEBUG: Redis pool monitoring started")


# =============================================================================
# Initialization and Cleanup
# =============================================================================


def start_redis_debugging(
    monitor_pools: bool = True, monitor_interval: int = 60
) -> None:
    """
    Start Redis debugging with optional pool monitoring.

    :param monitor_pools: Whether to monitor connection pools
    :param monitor_interval: How often to log pool statistics (seconds)
    """
    # Start stats tracking
    start_tracking_redis_stats()

    # Patch the client function
    patch_redis_client()

    # Start pool monitoring if requested
    if monitor_pools:
        monitor_redis_pools(monitor_interval)

    logger.info("REDIS_DEBUG: Redis debugging fully enabled")


# =============================================================================
# Global instance and module-level functions
# =============================================================================

# Create a global instance
redis_stats = RedisStats()


# Expose stats functionality to the module level
def start_tracking_redis_stats() -> None:
    """Start tracking Redis command statistics."""
    redis_stats.start_tracking()
    logger.info("Redis stats tracking enabled")


def stop_tracking_redis_stats() -> None:
    """Stop tracking Redis command statistics."""
    redis_stats.stop_tracking()
    logger.info("Redis stats tracking disabled")


def get_redis_stats_report(
    top_n: int = 20, stack_depth: int = DEFAULT_STACK_DEPTH, table_width: int = 140
) -> str:
    """
    Get a formatted report of Redis command statistics.

    :param top_n: Number of top commands to include in the report
    :param stack_depth: Number of call stack frames to show for each caller
    :param table_width: Maximum width for the table display
    :return: Formatted report
    """
    return redis_stats.get_report(top_n, stack_depth, table_width)


def get_sorted_redis_commands() -> list[tuple[str, str, CommandStats]]:
    """Get Redis commands sorted by total execution time."""
    return redis_stats.get_sorted_commands()


# Add this deeper patching function at the bottom of the file


def deep_patch_redis() -> None:
    """
    Patch Redis at a deeper level to guarantee all Redis operations are tracked.
    This patches the Redis module's core methods that all clients use.
    """
    import redis.client

    # Save the original method
    original_execute = redis.client.Redis._execute_command

    # Create our replacement method
    def patched_execute(self, *args, **options):
        """Patched execute_command for all Redis clients"""
        # Get command name
        command_name = args[0] if args else "unknown"

        # Get caller information using our existing utilities
        caller, call_stack = collect_call_stack()

        # Execute command and time it
        start_time = time.time()
        try:
            # Call the original Redis client method
            result = original_execute(self, *args, **options)
            elapsed = time.time() - start_time

            # Add to statistics - we're tracking even for non-DebugRedisClient instances
            redis_stats.add_stat(caller, call_stack, command_name, elapsed)

            # Only log slow commands to reduce noise
            if elapsed > SLOW_COMMAND_THRESHOLD:
                # Format arguments and results only for slow commands
                formatted_args = [format_arg(arg) for arg in args[1:]]
                formatted_kwargs = {
                    k: format_arg(v) for k, v in options.items() if k not in ["keys"]
                }
                _ = format_arg(result)

                logger.debug(
                    f"REDIS_SLOW: {command_name} took {elapsed:.6f}s - "
                    f"Args: ({', '.join(formatted_args)}, {formatted_kwargs}) - "
                    f"Called by: {caller}"
                )

            return result

        except Exception as e:
            # Handle and log exceptions
            elapsed = time.time() - start_time

            # Still record failed commands in stats
            redis_stats.add_stat(caller, call_stack, f"{command_name}_ERROR", elapsed)

            # Log the error with controlled output
            logger.error(
                f"REDIS_ERROR: {command_name} failed after {elapsed:.6f}s - "
                f"{type(e).__name__}: {e} - Called by: {caller}"
            )
            # Re-raise the original exception
            raise

    # Apply the patching directly to the Redis class
    redis.client.Redis._execute_command = patched_execute

    logger.info("REDIS_DEBUG: Applied deep patching to all Redis clients")
    return True


# Automatically start patching redis clients and tracking stats
# when this module is imported
deep_patch_redis()
start_tracking_redis_stats()
