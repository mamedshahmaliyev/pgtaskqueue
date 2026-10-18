"""Tasks with a dependency of their own (Pillow), run by a worker built for them.

The API never imports this module and the general worker's image does not contain Pillow.
What the rest of the system knows about these tasks is what this worker publishes when it
starts: their names, payload schemas and defaults.

    pip install -r examples/workers/image/requirements.txt
    tq worker --executors examples.executors.image --tasks 'image.*:2'

    tq enqueue image.render '{"width": 1600, "height": 900}' --wait
"""

from __future__ import annotations

import hashlib
import io
import tempfile
from pathlib import Path

from PIL import Image, ImageFilter
from pydantic import BaseModel, Field

from pgtaskqueue import Executor, NonRetryable, TaskContext, register

OUTPUT_DIR = Path(tempfile.gettempdir()) / "tq-images"


def _render(width: int, height: int) -> Image.Image:
    """A deterministic test pattern, so the example needs no input files."""
    red = Image.linear_gradient("L").resize((width, height))
    green = red.transpose(Image.Transpose.ROTATE_90).resize((width, height))
    blue = Image.radial_gradient("L").resize((width, height))
    return Image.merge("RGB", (red, green, blue))


def _save(image: Image.Image, name: str) -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    path = OUTPUT_DIR / name
    path.write_bytes(buffer.getvalue())
    return {
        "path": str(path),
        "width": image.width,
        "height": image.height,
        "bytes": buffer.tell(),
        "sha256": hashlib.sha256(buffer.getvalue()).hexdigest(),
    }


class RenderPayload(BaseModel):
    width: int = Field(default=1280, ge=16, le=8192)
    height: int = Field(default=720, ge=16, le=8192)
    blur: float = Field(default=0, ge=0, le=50)


@register
class Render(Executor[RenderPayload]):
    """Renders a test pattern to a PNG.

    Pillow is synchronous and CPU-bound, so the work goes to a thread with
    `ctx.to_thread` and the event loop stays free to heartbeat. That is also why this
    worker runs two slots, not twenty: see docs/workers.md#sizing-slots.
    """

    name = "image.render"
    payload_model = RenderPayload
    timeout = 120

    async def run(self, ctx: TaskContext, payload: RenderPayload) -> dict:
        def work() -> dict:
            image = _render(payload.width, payload.height)
            if payload.blur:
                image = image.filter(ImageFilter.GaussianBlur(payload.blur))
            return _save(image, f"{ctx.id}.png")

        result = await ctx.to_thread(work)
        ctx.log(f"rendered {result['width']}x{result['height']}, {result['bytes']} bytes")
        return result


class ThumbnailPayload(BaseModel):
    # The `path` from an image.render output.
    path: str
    max_size: int = Field(default=256, ge=16, le=2048)


@register
class Thumbnail(Executor[ThumbnailPayload]):
    """Scales an image this worker rendered down to fit a square of `max_size` pixels."""

    name = "image.thumbnail"
    payload_model = ThumbnailPayload
    timeout = 60

    async def run(self, ctx: TaskContext, payload: ThumbnailPayload) -> dict:
        def work() -> dict:
            source = Path(payload.path).resolve()
            # Payloads come from whoever may enqueue. Only read what this worker wrote.
            if OUTPUT_DIR.resolve() not in source.parents:
                raise NonRetryable(f"{payload.path} is outside {OUTPUT_DIR}")
            if not source.is_file():
                raise NonRetryable(f"{payload.path} does not exist on this worker")
            with Image.open(source) as image:
                image.thumbnail((payload.max_size, payload.max_size))
                return _save(image, f"{ctx.id}.thumb.png")

        return await ctx.to_thread(work)
