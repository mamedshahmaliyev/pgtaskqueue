from __future__ import annotations

from fastapi import APIRouter, Response, status

from pgtaskqueue.api.deps import State, Who
from pgtaskqueue.api.schemas import ScheduleIn
from pgtaskqueue.schemas import ScheduleOut

router = APIRouter(tags=["schedules"])


@router.get("/schedules", response_model=list[ScheduleOut])
async def list_schedules(who: Who, state: State) -> list[ScheduleOut]:
    """Periodic schedules, by name: those declared in code and those created here."""
    return [ScheduleOut.of(schedule) for schedule in await state.service.schedules(who)]


@router.get("/schedules/{name}", response_model=ScheduleOut)
async def get_schedule(name: str, who: Who, state: State) -> ScheduleOut:
    return ScheduleOut.of(await state.service.get_schedule(who, name))


@router.put("/schedules/{name}", response_model=ScheduleOut)
async def put_schedule(
    name: str, body: ScheduleIn, response: Response, who: Who, state: State
) -> ScheduleOut:
    """Create a schedule (201) or replace it whole (200). Whether it is paused is kept, and
    so is its next run unless the timing changed. 409 for one declared in code."""
    schedule = await state.service.put_schedule(who, name, body)
    if schedule.created:
        response.status_code = status.HTTP_201_CREATED
    return ScheduleOut.of(schedule)


@router.post("/schedules/{name}/pause", response_model=ScheduleOut)
async def pause_schedule(name: str, who: Who, state: State) -> ScheduleOut:
    """Stop a schedule firing. Tasks it has already enqueued are not touched."""
    return ScheduleOut.of(await state.service.set_schedule_paused(who, name, True))


@router.post("/schedules/{name}/resume", response_model=ScheduleOut)
async def resume_schedule(name: str, who: Who, state: State) -> ScheduleOut:
    """Undo pause. If a fire time passed meanwhile, the schedule fires once, now."""
    return ScheduleOut.of(await state.service.set_schedule_paused(who, name, False))


@router.delete("/schedules/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(name: str, who: Who, state: State) -> None:
    """Delete a schedule. One declared in code returns when a worker that declares it
    starts; pause it instead to keep it quiet."""
    await state.service.delete_schedule(who, name)
