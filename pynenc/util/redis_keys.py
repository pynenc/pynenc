from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..invocation.status import InvocationStatus


class Key:
    def __init__(self, prefix: Optional[str]) -> None:
        self.prefix: str = f"{prefix}:" if prefix else ""

    def invocation(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation:{invocation_id}"

    def task(self, task_id: str) -> str:
        return f"{self.prefix}task:{task_id}"

    def args(self, task_id: str, arg: str, val: str) -> str:
        return f"{self.prefix}task:{task_id}:arg:{arg}:val:{val}"

    def status(self, task_id: str, status: "InvocationStatus") -> str:
        return f"{self.prefix}task:{task_id}:status:{status}"

    def invocation_status(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_status:{invocation_id}"

    def call(self, call_id: str) -> str:
        return f"{self.prefix}call:{call_id}"

    def edge(self, call_id: str) -> str:
        return f"{self.prefix}edge:{call_id}"

    def waiting_for(self, invocation_id: str) -> str:
        return f"{self.prefix}waiting_for:{invocation_id}"

    def waited_by(self, invocation_id: str) -> str:
        return f"{self.prefix}waited_by:{invocation_id}"

    def history(self, invocation_id: str) -> str:
        return f"{self.prefix}history:{invocation_id}"

    def result(self, invocation_id: str) -> str:
        return f"{self.prefix}result:{invocation_id}"

    def exception(self, invocation_id: str) -> str:
        return f"{self.prefix}exception:{invocation_id}"
