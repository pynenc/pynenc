from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("/", response_class=HTMLResponse)
async def tasks_list(request: Request) -> HTMLResponse:
    """Display all registered tasks."""
    app = get_pynenc_instance()

    # Get all registered tasks
    tasks = list(app.tasks.values())

    return templates.TemplateResponse(
        "tasks/list.html",
        {
            "request": request,
            "title": "Tasks Monitor",
            "app_id": app.app_id,
            "tasks": tasks,
        },
    )


@router.get("/{task_id}", response_class=HTMLResponse)
async def task_detail(request: Request, task_id: str) -> HTMLResponse:
    """Display detailed information about a specific task."""
    app = get_pynenc_instance()

    # Try to find the task by ID
    task = app.get_task(task_id)

    if not task:
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Task Not Found",
                "message": f"No task found with ID: {task_id}",
            },
            status_code=404,
        )

    # Get active calls for this task (limit to 50)
    calls = []
    for invocation in app.orchestrator.get_existing_invocations(task=task):
        if invocation.call not in calls:
            calls.append(invocation.call)
            if len(calls) >= 50:
                break

    return templates.TemplateResponse(
        "tasks/detail.html",
        {
            "request": request,
            "title": f"Task {task_id}",
            "app_id": app.app_id,
            "task": task,
            "task_config": {
                "registration_concurrency": task.conf.registration_concurrency.name,
                "key_arguments": task.conf.key_arguments,
                "disable_cache_args": task.conf.disable_cache_args,
                "retry_times": task.conf.retry_times,
                "retry_delay_sec": task.conf.retry_delay_sec,
            },
            "calls": calls,
        },
    )
