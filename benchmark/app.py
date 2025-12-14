import asyncio
import os
import time
import psutil
from collections import deque
from typing import Iterable, Tuple

from pyjobkit import Engine, Worker
from logger_cfg import logger
from logging_config import setup_logging
from memory_db import create_engine, create_instrumented_executor

# Настраиваем логирование для чистого вывода
setup_logging()


class Metrics:
    processed: int = 0
    enqueued: int = 0
    last_processed: int = 0
    last_time: float = time.time()
    start_time: float = time.time()
    rps_history: deque = deque(maxlen=60)  # последние 60 секунд
    cpu_history: deque = deque(maxlen=60)  # CPU % за 60 секунд
    ram_history: deque = deque(maxlen=60)  # RAM MB за 60 секунд
    error_events: int = 0
    last_error: str | None = None


metrics = Metrics()


def load_settings() -> Tuple[int, int]:
    """Read configuration from the environment with safe defaults."""

    rate = int(os.getenv("ENQUEUE_RATE", "200"))
    concurrency = int(os.getenv("CONCURRENCY", "8"))

    logger.info(
        "Запускаем с memory backend, скорость постановки=%s/с, воркеров=%s",
        rate,
        concurrency,
    )

    return rate, concurrency


async def enqueuer(engine: Engine, rate: int):
    """Оптимизированный enqueuer с контролем CPU."""

    # Адаптивный интервал - не быстрее чем нужно
    base_interval = 1.0 / min(rate, 500)  # Максимум 500 enqueue/сек
    default_timeout_s = 3_000
    max_queue_size = 1000  # Не накапливать больше 1000 задач
    
    while True:
        try:
            # Контроль размера очереди - экономим CPU если очередь большая
            queue_size = metrics.enqueued - metrics.processed
            if queue_size > max_queue_size:
                await asyncio.sleep(0.1)  # Пауза если очередь переполнена
                continue
            
            await engine.enqueue(
                kind="subprocess",
                payload={"cmd": ["echo", "."]},  # Простая команда без bash
                timeout_s=default_timeout_s,
            )
            metrics.enqueued += 1

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            metrics.error_events += 1
            metrics.last_error = f"Не удалось поставить задачу: {exc}"
            await asyncio.sleep(1.0)
            continue

        await asyncio.sleep(base_interval)


async def worker_runner(engine: Engine, concurrency: int):
    """Вспомогательный раннер, перезапускающий воркера при сбоях."""

    worker = Worker(engine, max_concurrency=concurrency)
    logger.info("Запускаем воркер с concurrency=%s", concurrency)
    logger.info("Доступные исполнители в движке: %s", [ex.kind for ex in engine.executors.values()])
    
    while True:
        try:
            logger.info("Воркер начинает работу...")
            await worker.run()
        except asyncio.CancelledError:
            logger.info("Воркер получил сигнал остановки")
            raise
        except Exception as exc:
            metrics.error_events += 1
            metrics.last_error = f"Сбой воркера: {exc}"
            logger.exception("Воркер упал, пробуем перезапустить через секунду")
            await asyncio.sleep(1.0)


async def metrics_updater():
    process = psutil.Process()
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        delta = metrics.processed - metrics.last_processed
        rps = delta / max(now - metrics.last_time, 1e-6)

        # RPS
        metrics.rps_history.append(round(rps, 2))
        
        # CPU % (текущий процесс)
        cpu_percent = process.cpu_percent()
        metrics.cpu_history.append(round(cpu_percent, 1))
        
        # RAM MB (текущий процесс)
        ram_mb = process.memory_info().rss / 1024 / 1024
        metrics.ram_history.append(round(ram_mb, 1))
        
        metrics.last_processed = metrics.processed
        metrics.last_time = now


async def start_benchmark(
    rate: int, concurrency: int
) -> Tuple[Engine, Iterable[asyncio.Task]]:
    metrics.start_time = time.time()
    metrics.last_time = metrics.start_time
    metrics.last_processed = metrics.processed
    metrics.error_events = 0
    metrics.last_error = None

    logger.info("Создаем in-memory SQLite backend и запускаем движок")
    
    engine = await create_engine()

    tasks = (
        asyncio.create_task(enqueuer(engine, rate)),
        asyncio.create_task(worker_runner(engine, concurrency)),
        asyncio.create_task(metrics_updater()),
    )

    return engine, tasks


async def stop_tasks(tasks: Iterable[asyncio.Task]):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    rate, concurrency = load_settings()
    _engine, tasks = await start_benchmark(rate, concurrency)
    await asyncio.gather(*tasks)
