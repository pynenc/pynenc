"""Regression tests for timeline status-history rendering."""

import shutil
import subprocess

import pytest

from pynmon.app import templates


def _extract_between(source: str, start: str, end: str) -> str:
    """Return the template slice between two marker strings."""
    start_index = source.index(start)
    end_index = source.index(end, start_index)
    return source[start_index:end_index]


def test_timeline_status_history_rows_render_status_badges() -> None:
    """The JS side panel must never render blank status-history cells."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required to execute the timeline history renderer")
    assert node is not None

    rendered = templates.get_template(
        "invocations/partials/timeline_scripts.html"
    ).render()
    helpers = _extract_between(
        rendered,
        "  const STATUS_COLOR_STYLES",
        "  // \u2500\u2500 Cross-highlight helpers",
    )
    history_renderer = _extract_between(
        rendered,
        "  function buildHistoryRows",
        "  // \u2500\u2500 Invocation summary card helper",
    )
    node_source = "\n".join(
        [
            helpers,
            "function formatUTCDate(value) { return value; }",
            "function buildRunnerHtml() { return 'runner'; }",
            history_renderer,
            """
const html = buildHistoryRows([
  { status: "registered", timestamp: "2026-07-04T16:07:35.200Z" },
  { status: "pending", timestamp: "2026-07-04T16:07:35.216Z" },
  { status: "running", timestamp: "2026-07-04T16:07:35.230Z" },
  { status_record: { status: "retry" }, timestamp: "2026-07-04T16:07:35.248Z" },
  { status: "success", timestamp: "2026-07-04T16:07:35.260Z" },
  { timestamp: "2026-07-04T16:07:35.251Z" },
]);
const expectations = [
  '>registered</span>',
  ">pending</span>",
  ">running</span>",
  ">retry</span>",
  ">success</span>",
  ">unknown</span>",
  "background-color: #f39c12",
  "background-color: #3498db",
  "background-color: #9b59b6",
  "background-color: #27ae60",
  "background-color: #7f8c8d",
];
for (const expected of expectations) {
  if (!html.includes(expected)) {
    throw new Error(`Missing ${expected} in rendered history HTML:\\n${html}`);
  }
}
const badgeMatches = [...html.matchAll(/<span class="badge pynmon-status-badge badge-sm" style="([^"]+)">([^<]+)<\\/span>/g)];
const expectedText = new Map([
  ["registered", "background-color: #95a5a6; color: #ffffff;"],
  ["pending", "background-color: #f39c12; color: #ffffff;"],
  ["running", "background-color: #3498db; color: #ffffff;"],
  ["retry", "background-color: #9b59b6; color: #ffffff;"],
  ["success", "background-color: #27ae60; color: #ffffff;"],
  ["unknown", "background-color: #7f8c8d; color: #ffffff;"],
]);
for (const [label, style] of badgeMatches.map((match) => [match[2], match[1]])) {
  if (expectedText.get(label) !== style) {
    throw new Error(`Expected ${label} badge to use white text, got ${style}:\\n${html}`);
  }
  expectedText.delete(label);
}
if (expectedText.size) {
  throw new Error(`Missing badges for: ${[...expectedText.keys()].join(", ")}\\n${html}`);
}
""",
        ]
    )

    subprocess.run(
        [node, "-e", node_source],
        check=True,
        capture_output=True,
        text=True,
    )
