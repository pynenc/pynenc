"""
Shared fixtures for SVG timeline visualization tests.
"""

from datetime import UTC, datetime, timedelta

import pytest


from pynmon.util.svg import (
    InvocationBar,
    RunnerLane,
    TimelineBounds,
    TimelineConfig,
    TimelineData,
)
from pynmon.util.svg.sub_lane import (
    InvocationOnRunner,
    RunnerEntry,
)


# ============================================================================
# Common Time Fixtures
# ============================================================================


@pytest.fixture
def base_time() -> datetime:
    """Base time for test scenarios."""
    return datetime(2025, 11, 26, 21, 10, 43, tzinfo=UTC)


@pytest.fixture
def external_runner() -> str:
    """External runner ID."""
    return "ExternalRunner@host-1234"


@pytest.fixture
def thread_runner() -> str:
    """Thread runner ID."""
    return "ThreadRunner@host-1234"


# ============================================================================
# SVG Renderer Fixtures
# ============================================================================


@pytest.fixture
def config() -> TimelineConfig:
    """Standard configuration for tests."""
    return TimelineConfig(
        width=1000,
        lane_height=40,
        lane_padding=4,
        left_margin=200,
        top_margin=50,
        bar_height=24,
        min_bar_width=2,
    )


@pytest.fixture
def sample_bar() -> InvocationBar:
    """Sample invocation bar."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 0, 30, tzinfo=UTC)
    return InvocationBar(
        invocation_id="inv-001",
        start_time=start,
        end_time=end,
        status="RUNNING",
        color="#3498db",
        tooltip="Test invocation",
    )


@pytest.fixture
def sample_lane(sample_bar: InvocationBar) -> RunnerLane:
    """Sample runner lane with one bar."""
    lane = RunnerLane(
        runner_id="runner-1@host1",
        hostname="host1",
        label="runner-1",
        color="#e74c3c",
        lane_index=0,
    )
    lane.add_bar(sample_bar)
    return lane


@pytest.fixture
def sample_timeline_data(
    config: TimelineConfig, sample_lane: RunnerLane
) -> TimelineData:
    """Sample timeline data with one lane and one bar."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 1, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    data = TimelineData(bounds=bounds, config=config)
    data.lanes[sample_lane.runner_id] = sample_lane
    return data


@pytest.fixture
def empty_timeline_data(config: TimelineConfig) -> TimelineData:
    """Empty timeline data with no lanes."""
    now = datetime.now(UTC)
    bounds = TimelineBounds(start_time=now, end_time=now, config=config)
    return TimelineData(bounds=bounds, config=config)


@pytest.fixture
def multi_lane_data(config: TimelineConfig) -> TimelineData:
    """Timeline data with multiple lanes and bars."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    data = TimelineData(bounds=bounds, config=config)

    for i in range(3):
        lane = RunnerLane(
            runner_id=f"runner-{i}@host{i}",
            hostname=f"host{i}",
            label=f"runner-{i}",
            color=f"#{'ff' if i == 0 else '00'}{i}0{i}0",
            lane_index=i,
        )

        for j in range(2):
            bar_start = start + timedelta(seconds=30 * (i + j))
            bar_end = bar_start + timedelta(seconds=20)
            bar = InvocationBar(
                invocation_id=f"inv-{i}-{j}",
                start_time=bar_start,
                end_time=bar_end,
                status="RUNNING" if j == 0 else "SUCCESS",
                color="#3498db" if j == 0 else "#27ae60",
                tooltip=f"Invocation {i}-{j}",
            )
            lane.add_bar(bar)

        data.lanes[lane.runner_id] = lane

    return data


# ============================================================================
# Sub-lane Real Data Fixtures
# ============================================================================


def create_entry(ts: datetime, status: str) -> RunnerEntry:
    """Helper to create a RunnerEntry."""
    return RunnerEntry(timestamp=ts, status=status)


@pytest.fixture
def real_data_external_runner() -> str:
    """Real data external runner ID."""
    return "ExternalRunner@Krossovers-MacBook-Pro.local-1504"


@pytest.fixture
def real_data_thread_runner() -> str:
    """Real data thread runner ID."""
    return "ThreadRunner@Krossovers-MacBook-Pro.local-1504"


@pytest.fixture
def real_data(
    real_data_external_runner: str, real_data_thread_runner: str
) -> dict[str, list[InvocationOnRunner]]:
    """
    Real data captured from production showing the bug.

    ExternalRunner: 12 invocations, each with just REGISTERED status
    ThreadRunner: Same 12 invocations with PENDING -> RUNNING -> SUCCESS
    """
    external_runner = real_data_external_runner
    thread_runner = real_data_thread_runner

    external_invocations = [
        InvocationOnRunner(
            invocation_id="f9016541-37dc-4bc9-9edd-030da1a80224",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 44552, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="2003a485-24fb-45e5-a7c0-eef0e61871c7",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 51913, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="751ece6d-b68c-4144-8ea0-a07e151f4be9",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 572137, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="85301c58-3a2c-410b-a1b4-7dc5866b0e73",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 579959, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="f29f124c-edb3-46c0-b048-dce6d12de23b",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 586364, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="0bce6519-2b9a-4426-a0d8-6954a38c1879",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 109158, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="ac220f18-e117-43c3-924b-622f0eced6eb",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 120402, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="ea088988-2ab4-421d-8d40-1b029798f593",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 128063, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="fe8a4665-05aa-4fb3-914f-49b2505e57e2",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 135217, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="4ab5089c-b3a1-48a1-ab33-cb8e3645103b",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 143924, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="48977619-5423-425e-8dcd-3bc8fd2dbabb",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 149631, tzinfo=UTC), "registered"
                )
            ],
        ),
        InvocationOnRunner(
            invocation_id="61a7740a-6d19-4b9c-b664-5f5844180cb2",
            runner_id=external_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 156642, tzinfo=UTC), "registered"
                )
            ],
        ),
    ]

    thread_invocations = [
        InvocationOnRunner(
            invocation_id="f9016541-37dc-4bc9-9edd-030da1a80224",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 144759, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 146751, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 46, 149271, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="2003a485-24fb-45e5-a7c0-eef0e61871c7",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 146200, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 148589, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 46, 154762, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="751ece6d-b68c-4144-8ea0-a07e151f4be9",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 774524, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 777045, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 782805, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="85301c58-3a2c-410b-a1b4-7dc5866b0e73",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 777212, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 780360, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 786087, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="f29f124c-edb3-46c0-b048-dce6d12de23b",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 780503, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 43, 783726, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 789945, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="0bce6519-2b9a-4426-a0d8-6954a38c1879",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 204234, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 206329, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 828869, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="ac220f18-e117-43c3-924b-622f0eced6eb",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 205965, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 208280, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 689500, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="ea088988-2ab4-421d-8d40-1b029798f593",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 208416, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 210892, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 830072, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="fe8a4665-05aa-4fb3-914f-49b2505e57e2",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 210439, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 212990, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 45, 663939, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="4ab5089c-b3a1-48a1-ab33-cb8e3645103b",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 213461, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 214959, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 216779, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="48977619-5423-425e-8dcd-3bc8fd2dbabb",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 215041, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 217978, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 218510, tzinfo=UTC), "success"
                ),
            ],
        ),
        InvocationOnRunner(
            invocation_id="61a7740a-6d19-4b9c-b664-5f5844180cb2",
            runner_id=thread_runner,
            entries=[
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 428195, tzinfo=UTC), "pending"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 430651, tzinfo=UTC), "running"
                ),
                create_entry(
                    datetime(2025, 11, 26, 21, 10, 44, 435667, tzinfo=UTC), "failed"
                ),
            ],
        ),
    ]

    return {
        external_runner: external_invocations,
        thread_runner: thread_invocations,
    }


@pytest.fixture
def real_data_end_time() -> datetime:
    """End time for real data tests."""
    return datetime(2025, 11, 26, 21, 10, 47, tzinfo=UTC)
