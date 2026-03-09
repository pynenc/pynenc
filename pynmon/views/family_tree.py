"""Family tree view for invocation hierarchy visualization.

Provides an HTMX-loadable endpoint that renders the parent-child tree
for any invocation, reused from both the invocation detail page and
the workflow detail page.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.identifiers.invocation_id import InvocationId
from pynmon.app import get_pynenc_instance, templates
from pynmon.util.family_tree import build_family_tree
from pynmon.util.family_tree_svg import render_family_tree_svg

router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.family_tree")


def _build_svg(
    invocation_id: InvocationId,
    expand_ids: frozenset[str] = frozenset(),
) -> str | None:
    """Build the family tree SVG for an invocation (sync, runs in thread).

    :param invocation_id: Focus invocation ID.
    :param expand_ids: Invocation IDs whose subtrees should be expanded.
    :return: SVG string, or None on failure.
    """
    app = get_pynenc_instance()
    try:
        root = build_family_tree(
            app.state_backend,
            invocation_id,
            expand_ids=expand_ids,
        )
        if root is None:
            return None
        return render_family_tree_svg(root, focus_id=str(invocation_id))
    except Exception as exc:
        logger.warning("Family tree build failed for %s: %s", invocation_id, exc)
        return None


@router.get("/{invocation_id}/family-tree", response_class=HTMLResponse)
async def invocation_family_tree(
    request: Request,
    invocation_id: InvocationId,
    bare: int = 0,
    expand: str = "",
) -> HTMLResponse:
    """Return an HTML partial with the family tree SVG for HTMX lazy loading.

    When ``bare=1`` the response contains only the raw SVG wrapped in a
    minimal scrollable container — useful for injecting into a panel that
    already provides its own chrome (e.g. the floating family-tree panel
    on the detail page).

    When ``expand`` is provided (comma-separated invocation IDs), those
    subtrees receive an extra node budget so the tree progressively grows
    as the user clicks "load more" badges.

    :param request: FastAPI request object.
    :param invocation_id: The invocation to centre the tree on.
    :param bare: If truthy, skip the card wrapper.
    :param expand: Comma-separated invocation IDs to expand beyond default limits.
    :return: Rendered family tree partial.
    """
    expand_ids = frozenset(eid.strip() for eid in expand.split(",") if eid.strip())
    svg_content = await asyncio.to_thread(_build_svg, invocation_id, expand_ids)
    template = "partials/family_tree_bare.html" if bare else "partials/family_tree.html"
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "family_tree_svg": svg_content,
        },
    )
