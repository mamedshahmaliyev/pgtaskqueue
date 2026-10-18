# One Dockerfile, three kinds of image, chosen with build arguments:
#
#   EXTRAS=api            the HTTP API (FastAPI, JWT, MCP). No executor code.
#   (nothing)             a worker with only the core dependencies.
#   REQUIREMENTS=<file>   a worker with its own extra dependencies on top — the point of
#                         running each kind of work in its own image.
#
# See docs/infrastructure.md#images.
ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, against a stub package, so editing source does not reinstall them.
ARG EXTRAS=""
COPY pyproject.toml README.md LICENSE ./
# The version is read from this file at build time; it changes rarely enough to cache on.
COPY src/pgtaskqueue/_version.py src/pgtaskqueue/_version.py
RUN touch src/pgtaskqueue/__init__.py && pip install -e ".${EXTRAS:+[${EXTRAS}]}"

# A worker's own requirements: whatever its executors import and the others do not.
ARG REQUIREMENTS=""
COPY examples ./examples
RUN if [ -n "${REQUIREMENTS}" ]; then pip install -r "${REQUIREMENTS}"; fi

COPY src ./src
COPY benchmarks ./benchmarks

RUN useradd --create-home --uid 10001 tq
USER tq

# Overridden per service in compose.yml.
CMD ["tq", "worker"]
