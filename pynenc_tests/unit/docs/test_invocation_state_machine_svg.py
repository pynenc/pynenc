import re

from pynenc.invocation.status import InvocationStatus
from pynenc.invocation.status import _CONFIG
from pynenc.invocation.status_graph import DEFAULT_OUTPUT_PATH, render_svg


def test_invocation_state_machine_svg_is_up_to_date() -> None:
    assert DEFAULT_OUTPUT_PATH.read_text(encoding="utf-8") == render_svg()


def test_invocation_state_machine_svg_contains_all_statuses() -> None:
    svg = render_svg()
    for status in InvocationStatus:
        assert status.name in svg


def test_invocation_state_machine_svg_contains_all_transitions() -> None:
    svg = render_svg()
    assert 'data-edge="START->REGISTERED"' in svg
    for source, definition in _CONFIG.definitions.items():
        if source is None:
            continue
        for target in definition.allowed_transitions:
            assert f'data-edge="{source.name}->{target.name}"' in svg


def test_invocation_state_machine_svg_uses_direct_lines() -> None:
    svg = render_svg()
    edge_groups = re.findall(r'<g data-edge="[^"]+">\n(.*?)</g>', svg, re.DOTALL)
    assert edge_groups
    for group in edge_groups:
        assert group.count("<line ") == 2
        assert "<path " not in group
