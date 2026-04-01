"""
Performance benchmarks for pynenc core operations.

These benchmarks cover the performance-critical paths in the pynenc codebase:
- Serialization and deserialization (JSON serializer)
- Argument hashing (compute_args_id)
- Status transition validation (invocation state machine)
- Task call creation and argument binding
- Builder pattern for app configuration
"""

from enum import StrEnum


from pynenc.arguments import Arguments
from pynenc.call import compute_args_id
from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    status_record_transition,
    validate_transition,
    validate_ownership,
)
from pynenc.serializer.json_serializer import (
    JsonSerializer,
    _preprocess_for_json,
)


# ---------------------------------------------------------------------------
# Serialization benchmarks
# ---------------------------------------------------------------------------


class SampleEnum(StrEnum):
    VALUE_A = "a"
    VALUE_B = "b"


SIMPLE_DICT = {"x": 1, "y": "hello", "z": [1, 2, 3]}
NESTED_DICT = {
    "level1": {"level2": {"level3": {"data": list(range(50))}}},
    "items": [{"id": i, "value": f"item_{i}"} for i in range(20)],
}
LARGE_LIST = list(range(500))
ENUM_DICT = {"status": SampleEnum.VALUE_A, "other": SampleEnum.VALUE_B}


def test_serialize_simple_dict(benchmark):
    """Benchmark serialization of a small flat dictionary."""
    benchmark(JsonSerializer.serialize, SIMPLE_DICT)


def test_deserialize_simple_dict(benchmark):
    """Benchmark deserialization of a small flat dictionary."""
    serialized = JsonSerializer.serialize(SIMPLE_DICT)
    benchmark(JsonSerializer.deserialize, serialized)


def test_serialize_nested_dict(benchmark):
    """Benchmark serialization of a nested dictionary with lists."""
    benchmark(JsonSerializer.serialize, NESTED_DICT)


def test_deserialize_nested_dict(benchmark):
    """Benchmark deserialization of a nested dictionary with lists."""
    serialized = JsonSerializer.serialize(NESTED_DICT)
    benchmark(JsonSerializer.deserialize, serialized)


def test_serialize_large_list(benchmark):
    """Benchmark serialization of a large list."""
    benchmark(JsonSerializer.serialize, LARGE_LIST)


def test_serialize_enum_values(benchmark):
    """Benchmark serialization of enum values (requires preprocessing)."""
    benchmark(JsonSerializer.serialize, ENUM_DICT)


def test_deserialize_enum_values(benchmark):
    """Benchmark round-trip deserialization of enum values."""
    serialized = JsonSerializer.serialize(ENUM_DICT)
    benchmark(JsonSerializer.deserialize, serialized)


def test_preprocess_for_json(benchmark):
    """Benchmark the enum preprocessing step."""
    data = {"status": SampleEnum.VALUE_A, "items": [SampleEnum.VALUE_B] * 10}
    benchmark(_preprocess_for_json, data)


# ---------------------------------------------------------------------------
# Argument hashing benchmarks
# ---------------------------------------------------------------------------


SMALL_ARGS = {"x": "1", "y": "hello"}
MEDIUM_ARGS = {f"arg_{i}": f"value_{i}" for i in range(10)}
LARGE_ARGS = {f"arg_{i}": f"value_{i}" * 50 for i in range(20)}


def test_compute_args_id_small(benchmark):
    """Benchmark argument ID computation for a small set of arguments."""
    benchmark(compute_args_id, SMALL_ARGS)


def test_compute_args_id_medium(benchmark):
    """Benchmark argument ID computation for a medium set of arguments."""
    benchmark(compute_args_id, MEDIUM_ARGS)


def test_compute_args_id_large(benchmark):
    """Benchmark argument ID computation for large arguments."""
    benchmark(compute_args_id, LARGE_ARGS)


def test_compute_args_id_empty(benchmark):
    """Benchmark argument ID computation for empty arguments."""
    benchmark(compute_args_id, {})


# ---------------------------------------------------------------------------
# Status transition benchmarks
# ---------------------------------------------------------------------------


def test_validate_transition_valid(benchmark):
    """Benchmark validation of a valid status transition."""
    benchmark(
        validate_transition, InvocationStatus.REGISTERED, InvocationStatus.PENDING
    )


def test_status_is_final(benchmark):
    """Benchmark checking if a status is final."""
    benchmark(InvocationStatus.SUCCESS.is_final)


def test_status_can_transition_to(benchmark):
    """Benchmark checking if a transition is valid."""
    benchmark(InvocationStatus.REGISTERED.can_transition_to, InvocationStatus.PENDING)


def test_status_record_transition_registered_to_pending(benchmark):
    """Benchmark a full status record transition from REGISTERED to PENDING."""
    record = InvocationStatusRecord(status=InvocationStatus.REGISTERED, runner_id=None)
    benchmark(
        status_record_transition,
        record,
        InvocationStatus.PENDING,
        "runner-1",
    )


def test_status_record_transition_running_to_success(benchmark):
    """Benchmark a full status record transition from RUNNING to SUCCESS."""
    record = InvocationStatusRecord(
        status=InvocationStatus.RUNNING, runner_id="runner-1"
    )
    benchmark(
        status_record_transition,
        record,
        InvocationStatus.SUCCESS,
        "runner-1",
    )


def test_validate_ownership_with_owner(benchmark):
    """Benchmark ownership validation when ownership is required."""
    record = InvocationStatusRecord(
        status=InvocationStatus.RUNNING, runner_id="runner-1"
    )
    benchmark(validate_ownership, record, InvocationStatus.SUCCESS, "runner-1")


# ---------------------------------------------------------------------------
# InvocationStatusRecord serialization benchmarks
# ---------------------------------------------------------------------------


def test_status_record_to_json(benchmark):
    """Benchmark InvocationStatusRecord serialization to JSON."""
    record = InvocationStatusRecord(
        status=InvocationStatus.RUNNING, runner_id="runner-1"
    )
    benchmark(record.to_json)


def test_status_record_from_json(benchmark):
    """Benchmark InvocationStatusRecord deserialization from JSON."""
    record = InvocationStatusRecord(
        status=InvocationStatus.RUNNING, runner_id="runner-1"
    )
    json_data = record.to_json()
    benchmark(InvocationStatusRecord.from_json, json_data)


# ---------------------------------------------------------------------------
# Arguments benchmarks
# ---------------------------------------------------------------------------


def _sample_func(x: int, y: str, z: list) -> None:
    pass


def test_arguments_from_call(benchmark):
    """Benchmark creating Arguments from a function call."""
    benchmark(Arguments.from_call, _sample_func, 42, "hello", [1, 2, 3])


def test_arguments_from_call_with_kwargs(benchmark):
    """Benchmark creating Arguments from keyword arguments."""
    benchmark(Arguments.from_call, _sample_func, x=42, y="hello", z=[1, 2, 3])


def test_arguments_str_representation(benchmark):
    """Benchmark string representation of arguments."""
    args = Arguments(kwargs={"x": 42, "y": "hello", "z": [1, 2, 3]})
    benchmark(str, args)


# ---------------------------------------------------------------------------
# Builder benchmarks
# ---------------------------------------------------------------------------


def test_builder_memory_setup(benchmark):
    """Benchmark building a memory-backed Pynenc app."""
    from pynenc.builder import PynencBuilder

    def build_app():
        return PynencBuilder().app_id("bench_app").memory().dev_mode().build()

    benchmark(build_app)
