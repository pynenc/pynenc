from copy import deepcopy
from dataclasses import dataclass
from datetime import date

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType


@dataclass
class BacktestMetadata:
    start_date: date
    end_date: date
    frequency: str


@dataclass
class Parameter:
    value: str


@dataclass
class CoreParameters:
    backtest: Parameter
    frequency_map: dict[str, str]


@dataclass
class MetadataContainer:
    backtest_metadata: BacktestMetadata
    core_parameters: CoreParameters


# Create app for testing
app = Pynenc(
    config_values={
        "serializer_cls": "PickleSerializer",
        "arg_cache": "MemArgCache",
        "min_size_to_cache": 0,
    }
)


@app.task(registration_concurrency=ConcurrencyControlType.DISABLED)
def process_chunk_task(metadata: MetadataContainer, instrument_container: str) -> dict:
    """Process a chunk of dates rebuilding context locally."""
    # Return the dates from metadata for verification
    return {
        "start_date": metadata.backtest_metadata.start_date,
        "end_date": metadata.backtest_metadata.end_date,
        "frequency": metadata.backtest_metadata.frequency,
    }


def split_metadata(
    metadata: MetadataContainer, num_chunks: int
) -> list[dict[str, MetadataContainer]]:
    """Split a MetadataContainer into multiple containers with different date ranges."""
    # Create date ranges for each chunk (simplified)
    metadata_chunks: list[dict[str, MetadataContainer]] = []

    # Starting month and day ranges
    days = [
        (1, 15),  # month 1, days 1-15
        (16, 31),  # month 1, days 16-31
        (1, 15),  # month 2, days 1-15
        (16, 28),  # month 2, days 16-28
        (1, 15),  # month 3, days 1-15
    ]

    for i, (start_day, end_day) in enumerate(days[:num_chunks]):
        # Create a deep copy of the metadata to avoid modifying the original
        new_metadata = deepcopy(metadata)

        # Update the start and end dates for this chunk
        month = (i // 2) + 1  # Month 1, 1, 2, 2, 3, ...
        new_metadata.backtest_metadata.start_date = date(2024, month, start_day)
        new_metadata.backtest_metadata.end_date = date(2024, month, end_day)

        print(
            f"Created chunk {i+1}: {new_metadata.backtest_metadata.start_date} to {new_metadata.backtest_metadata.end_date}"
        )
        metadata_chunks.append({"metadata": new_metadata})

    return metadata_chunks


def test_parallelize_with_deepcopy() -> None:
    """Test that parallelize correctly handles deep copies of nested objects."""
    # Create original metadata
    original_metadata = MetadataContainer(
        backtest_metadata=BacktestMetadata(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 3, 31),
            frequency="daily",
        ),
        core_parameters=CoreParameters(
            backtest=Parameter(value="test"),
            frequency_map={"daily": "D", "weekly": "W"},
        ),
    )

    instrument_container = "test_instrument"

    # Split metadata into chunks
    metadata_chunks = split_metadata(original_metadata, 5)

    # Expected date ranges after splitting
    expected_dates = [
        (date(2024, 1, 1), date(2024, 1, 15)),
        (date(2024, 1, 16), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 15)),
        (date(2024, 2, 16), date(2024, 2, 28)),
        (date(2024, 3, 1), date(2024, 3, 15)),
    ]

    # Use parallelize with common_args
    invocation_group = process_chunk_task.parallelize(
        param_iter=metadata_chunks,
        common_args={"instrument_container": instrument_container},
    )

    # Check invocations - here's where the issue would occur
    for i, invocation in enumerate(invocation_group.invocations):
        metadata = invocation.arguments.kwargs["metadata"]

        # This assertion will fail if all invocations have the same date
        expected_start, expected_end = expected_dates[i]

        print(
            f"Invocation {i} dates: {metadata.backtest_metadata.start_date} to {metadata.backtest_metadata.end_date}"
        )

        assert metadata.backtest_metadata.start_date == expected_start, (
            f"Invocation {i} has wrong start date: "
            f"{metadata.backtest_metadata.start_date} != {expected_start}"
        )
        assert metadata.backtest_metadata.end_date == expected_end, (
            f"Invocation {i} has wrong end date: "
            f"{metadata.backtest_metadata.end_date} != {expected_end}"
        )

        # Verify common args is present
        assert (
            invocation.arguments.kwargs["instrument_container"] == instrument_container
        )
