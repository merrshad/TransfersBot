# syntax=docker/dockerfile:1.7
#
# Multi-stage build: dependencies are compiled/installed in a builder image and
# only the virtualenv is copied into the slim runtime. The process runs as an
# unprivileged user and is stopped by SIGTERM (handled cooperatively by main).

FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Build tools are only present in this stage. They are needed for the rare
# dependency that has no manylinux wheel; the runtime image stays slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY core ./core
COPY storage ./storage
COPY adapters ./adapters
COPY config.py logging_setup.py main.py healthcheck.py ./

RUN pip install --upgrade pip \
    && pip install ".[media]"


FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    APP_HOME=/app \
    HEALTHCHECK_FILE=/app/data/health \
    HEALTHCHECK_MAX_AGE_SECONDS=90

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app \
       --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=app:app core ./core
COPY --chown=app:app storage ./storage
COPY --chown=app:app adapters ./adapters
COPY --chown=app:app config.py logging_setup.py main.py healthcheck.py ./

# Both paths are Docker volume mount points. Keeping them owned by the runtime
# user lets a fresh named volume inherit the right permissions.
RUN mkdir -p /app/data /app/media \
    && chown -R app:app /app/data /app/media

USER app

# No ports: the bridge uses outbound long polling only.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "healthcheck.py"]

CMD ["python", "main.py"]
