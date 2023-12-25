from typing import TYPE_CHECKING

import redis

if TYPE_CHECKING:
    from ..invocation.status import InvocationStatus


def sanitize_for_redis(s: str) -> str:
    replacements = {
        "[": "__OPEN_BRACKET__",
        "]": "__CLOSE_BRACKET__",
        "*": "__ASTERISK__",
    }

    for k, v in replacements.items():
        s = s.replace(k, v)

    return s


class Key:
    def __init__(self, app_id: str, prefix: str) -> None:
        app_id = sanitize_for_redis(app_id)
        prefix = sanitize_for_redis(prefix)
        if not prefix:
            raise ValueError("Prefix cannot be an empty string or None")
        if prefix and not prefix.endswith(":"):
            prefix += ":"
        if app_id:
            prefix = f"{app_id}:{prefix}"
        self.prefix = f"__pynenc__:{prefix}"

    def invocation(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation:{invocation_id}"

    def task(self, task_id: str) -> str:
        return f"{self.prefix}task:{task_id}"

    def args(self, task_id: str, arg: str, val: str) -> str:
        return f"{self.prefix}task:{task_id}:arg:{arg}:val:{val}"

    def status(self, task_id: str, status: "InvocationStatus") -> str:
        return f"{self.prefix}task:{task_id}:status:{status}"

    def pending_timer(self, invocation_id: str) -> str:
        return f"{self.prefix}pending_timer:{invocation_id}"

    def previous_status(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_previous_status:{invocation_id}"

    def invocation_status(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_status:{invocation_id}"

    def invocation_retries(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_retries:{invocation_id}"

    def call(self, call_id: str) -> str:
        return f"{self.prefix}call:{call_id}"

    def call_to_invocation(self, call_id: str) -> str:
        return f"{self.prefix}call_to_invocation:{call_id}"

    def edge(self, call_id: str) -> str:
        return f"{self.prefix}edge:{call_id}"

    def waiting_for(self, invocation_id: str) -> str:
        return f"{self.prefix}waiting_for:{invocation_id}"

    def waited_by(self, invocation_id: str) -> str:
        return f"{self.prefix}waited_by:{invocation_id}"

    def all_waited(self) -> str:
        return f"{self.prefix}all_waited"

    def not_waiting(self) -> str:
        return f"{self.prefix}not_waiting"

    def history(self, invocation_id: str) -> str:
        return f"{self.prefix}history:{invocation_id}"

    def result(self, invocation_id: str) -> str:
        return f"{self.prefix}result:{invocation_id}"

    def exception(self, invocation_id: str) -> str:
        return f"{self.prefix}exception:{invocation_id}"

    def invocation_auto_purge(self) -> str:
        return f"{self.prefix}invocation_auto_purge"

    def default_queue(self) -> str:
        return f"{self.prefix}default_queue"

    def purge(self, client: redis.Redis) -> None:
        """Purge all keys with the given prefix"""
        scan = client.scan_iter(self.prefix + "*")
        for key in scan:
            client.delete(key)
