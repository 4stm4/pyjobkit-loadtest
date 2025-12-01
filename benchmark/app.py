import asyncio
import logging
import os
import time
from collections import deque
from pathlib import Path
from typing import Iterable, Tuple

from pyjobkit import Engine, Worker
from pyjobkit.backends.sql import SQLBackend
from pyjobkit.executors import SubprocessExecutor
from sqlalchemy.engine import make_url
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    JSON,
    MetaData,
    Text,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.sql.schema import Table


LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()


def configure_logging() -> None:
    """Configure verbose logging for easier debugging."""

    level = getattr(logging, LOG_LEVEL, logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Make sure pyjobkit internals are also verbose when debugging issues.
    logging.getLogger("pyjobkit").setLevel(level)
    logging.getLogger("pyjobkit.backends.sql").setLevel(level)

    local_logger = logging.getLogger(__name__)
    local_logger.debug(
        "Логирование инициализировано: уровень=%s", logging.getLevelName(level)
    )


configure_logging()

logger = logging.getLogger(__name__)


class Metrics:
    """Thread-safe-ish metrics storage used across coroutines."""

    processed: int = 0
    enqueued: int = 0
    last_processed: int = 0
    last_time: float = time.time()
    start_time: float = time.time()
    rps_history: deque = deque(maxlen=60)  # последние 60 секунд
    error_events: int = 0
    last_error: str | None = None


DEFAULT_DSN = "sqlite+aiosqlite:///./jobkit.db"

metrics = Metrics()


metadata = MetaData()

job_tasks = Table(
    "job_tasks",
    metadata,
    Column("id", Text, primary_key=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column(
        "scheduled_for",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    ),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", Text, nullable=False, server_default=text("'queued'")),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("max_attempts", Integer, nullable=False, server_default=text("25")),
    Column("priority", Integer, nullable=False, server_default=text("100")),
    Column("kind", Text, nullable=False),
    Column("payload", JSON().with_variant(Text(), "sqlite"), nullable=False),
    Column("result", JSON().with_variant(Text(), "sqlite")),
    Column("idempotency_key", Text, unique=True),
    Column("cancel_requested", Integer, nullable=False, server_default=text("0")),
    Column("leased_by", Text),
    Column("lease_until", DateTime(timezone=True)),
    Column("version", Integer, nullable=False, server_default=text("0")),
    Column("timeout_s", Integer, nullable=False, server_default=text("300")),
)

Index(
    "idx_job_tasks_queue",
    job_tasks.c.status,
    job_tasks.c.scheduled_for,
    job_tasks.c.lease_until,
)


async def ensure_schema(sql_engine: AsyncEngine):
    """Create required tables for the SQL backend if they are missing."""

    async with sql_engine.begin() as conn:
        if sql_engine.url.get_backend_name() == "sqlite":
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
            await conn.exec_driver_sql("PRAGMA busy_timeout=30000")
        await conn.run_sync(metadata.create_all)


def ensure_sqlite_directory(dsn: str) -> None:
    """Create the parent directory for a SQLite database if needed."""

    url = make_url(dsn)
    if url.get_backend_name() != "sqlite":
        return

    database = url.database
    if not database or database == ":memory:":
        return

    db_path = Path(database).expanduser()
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)


class InstrumentedSubprocessExecutor(SubprocessExecutor):
    """Executor that increments processed metrics after successful runs."""

    async def execute(self, job):
        try:
            result = await super().execute(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            metrics.error_events += 1
            metrics.last_error = f"Ошибка исполнения {type(exc).__name__}: {exc}"
            logger.exception("Воркер не смог выполнить задачу")
            raise

        metrics.processed += 1
        return result


class LeaseAwareSQLBackend(SQLBackend):
    """SQL backend compatible with pyjobkit 0.2.0 optimistic locking.

    In pyjobkit 0.2.0 the lease query increments the `version` column in the
    database, but the returned ``Job`` object still holds the pre-lease
    version. When the worker later tries to finish the job, the optimistic
    locking check compares the stale version with the incremented one in the
    database and fails, leaving the task "running" until the lease expires.

    By manually bumping the in-memory version to match the database after the
    lease (including batch leases), we keep the worker and the persisted
    record in sync while staying on the supported 0.2.0 release.
    """

    async def lease(self, worker_id: str):
        job = await super().lease(worker_id)
        if job and job.version is not None:
            job.version += 1
        return job

    async def lease_batch(self, worker_id: str, limit: int):
        jobs = await super().lease_batch(worker_id, limit)
        for job in jobs:
            if job.version is not None:
                job.version += 1
        return jobs


def load_settings() -> Tuple[str, int, int]:
    """Read configuration from the environment with safe defaults."""

    dsn = os.getenv("DSN")
    if not dsn:
        logging.warning(
            "DSN environment variable is not set; falling back to %s", DEFAULT_DSN
        )
        dsn = DEFAULT_DSN

    rate = int(os.getenv("ENQUEUE_RATE", "100"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))

    safe_url = make_url(dsn).render_as_string(hide_password=True)
    logger.info(
        "Запускаем с DSN=%s, скорость постановки=%s/с, воркеров=%s",
        safe_url,
        rate,
        concurrency,
    )

    return dsn, rate, concurrency


async def enqueuer(engine: Engine, rate: int):
    """Постоянно ставит в очередь задачи с заданной скоростью."""

    interval = 1.0 / rate
    default_timeout_s = 300  # to avoid NULL timeout values when enqueuing
    while True:
        try:
            await engine.enqueue(
                kind="subprocess",
                # Запускаем через bash, чтобы гарантированно выполнить команду как единую строку
                payload={"cmd": ["bash", "-lc", "echo . && sleep 0.005"]},
                timeout_s=default_timeout_s,
            )
            metrics.enqueued += 1

            if metrics.enqueued <= 5 or metrics.enqueued % max(rate, 1) == 0:
                logger.debug(
                    "Поставлено задач всего: %s (порция из %s/с)",
                    metrics.enqueued,
                    rate,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            metrics.error_events += 1
            metrics.last_error = f"Не удалось поставить задачу: {exc}"
            logger.exception("Не удалось поставить задачу в очередь, повтор через секунду")
            await asyncio.sleep(1.0)
            continue

        await asyncio.sleep(interval)


async def worker_runner(engine: Engine, concurrency: int):
    """Вспомогательный раннер, перезапускающий воркера при сбоях."""

    worker = Worker(engine, max_concurrency=concurrency)
    logger.info("Запускаем воркер с concurrency=%s", concurrency)
    while True:
        try:
            await worker.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            metrics.error_events += 1
            metrics.last_error = f"Сбой воркера: {exc}"
            logger.exception("Воркер упал, пробуем перезапустить через секунду")
            await asyncio.sleep(1.0)


async def metrics_updater():
    """Каждую секунду считает RPS и сохраняет в историю."""

    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        delta = metrics.processed - metrics.last_processed
        rps = delta / max(now - metrics.last_time, 1e-6)

        metrics.rps_history.append(round(rps, 2))
        metrics.last_processed = metrics.processed
        metrics.last_time = now


async def start_benchmark(
    dsn: str, rate: int, concurrency: int
) -> Tuple[Engine, Iterable[asyncio.Task]]:
    """Create engine, workers and background tasks for the benchmark."""

    metrics.start_time = time.time()
    metrics.last_time = metrics.start_time
    metrics.last_processed = metrics.processed
    metrics.error_events = 0
    metrics.last_error = None

    ensure_sqlite_directory(dsn)
    url = make_url(dsn)
    connect_args = {"timeout": 30} if url.get_backend_name() == "sqlite" else {}

    logger.info(
        "Готовим backend %s (lease_ttl_s=60)",
        url.render_as_string(hide_password=True),
    )
    sql_engine: AsyncEngine = create_async_engine(dsn, connect_args=connect_args)
    await ensure_schema(sql_engine)
    logger.info("Схема базы проверена, запускаем движок и фоновые задачи")
    backend = LeaseAwareSQLBackend(sql_engine, lease_ttl_s=60)
    engine = Engine(backend=backend, executors=[InstrumentedSubprocessExecutor()])

    tasks = (
        asyncio.create_task(enqueuer(engine, rate)),
        asyncio.create_task(worker_runner(engine, concurrency)),
        asyncio.create_task(metrics_updater()),
    )

    return engine, tasks


async def stop_tasks(tasks: Iterable[asyncio.Task]):
    """Cancel background tasks gracefully."""

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    dsn, rate, concurrency = load_settings()

    _engine, tasks = await start_benchmark(dsn, rate, concurrency)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Для запуска через uvicorn в веб-режиме — не запускаем main()
    pass
