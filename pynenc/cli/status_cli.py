import argparse
import sys
from pathlib import Path

from pynenc.invocation.status_graph import DEFAULT_OUTPUT_PATH, render_svg, render_text


def add_status_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Add invocation status inspection commands to the main CLI."""
    status_parser = subparsers.add_parser(
        "status", help="Inspect the invocation status state machine"
    )
    status_subparsers = status_parser.add_subparsers(
        dest="status_command", required=True
    )

    render_parser = status_subparsers.add_parser(
        "render", help="Render the invocation status state machine"
    )
    render_parser.add_argument(
        "--format",
        choices=("text", "svg"),
        default="text",
        help="Output format (default: text)",
    )
    render_parser.add_argument(
        "--output",
        type=Path,
        help=(
            "File to write or check. Defaults to stdout, except SVG --check uses "
            f"the docs SVG path ({DEFAULT_OUTPUT_PATH})."
        ),
    )
    render_parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the output file does not match the current state machine",
    )
    render_parser.set_defaults(func=render_status_command, requires_app=False)


def render_status_command(args: argparse.Namespace) -> None:
    """Render or check the invocation status state machine."""
    rendered = render_svg() if args.format == "svg" else render_text()
    output: Path | None = args.output
    if args.check:
        if output is None:
            if args.format == "svg":
                output = DEFAULT_OUTPUT_PATH
            else:
                print("--check requires --output when --format text", file=sys.stderr)
                sys.exit(1)
        if not output.exists():
            print(f"Missing generated file: {output}", file=sys.stderr)
            sys.exit(1)
        if output.read_text(encoding="utf-8") != rendered:
            print(
                f"{output} is stale. Run: pynenc status render --format {args.format} --output {output}",
                file=sys.stderr,
            )
            sys.exit(1)
        return

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        print(f"Wrote {output}")
        return

    print(rendered, end="")
