import asyncio
import time
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from app import (
    load_settings,
    metrics,
    start_benchmark,
    stop_tasks,
)
from logger_cfg import configure_logging, logger
from memory_db import get_redis, _debug_incrby_count, _debug_incrby_total

configure_logging()

# Используем абсолютный путь к templates
current_dir = Path(__file__).parent
templates_dir = current_dir / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

async def get_redis_counter() -> int:
    """Получаем счётчик из Redis для проверки"""
    try:
        dsn = os.getenv('DSN', '')
        if not dsn.startswith('redis://'):
            return -1  # Redis не используется
        redis = await get_redis()
        val = await redis.get("pyjobkit:processed")
        return int(val) if val else 0
    except:
        return -1

# Глобальные переменные для benchmark
benchmark_task = None
engine = None

async def init_benchmark():
    """Инициализация benchmark с memory backend"""
    global benchmark_task, engine
    
    if benchmark_task is None:
        rate, concurrency = load_settings()
        
        # Сбрасываем Redis счётчик при старте (если используется Redis)
        dsn = os.getenv('DSN', '')
        if dsn.startswith('redis://'):
            try:
                redis = await get_redis()
                await redis.set("pyjobkit:processed", 0)
                logger.info("Redis счётчик сброшен на 0")
            except Exception as e:
                logger.warning(f"Не удалось сбросить Redis счётчик: {e}")
        
        try:
            engine, tasks = await start_benchmark(rate, concurrency)
            # Создаем единую фоновую задачу
            benchmark_task = asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Benchmark запущен успешно")
        except Exception as e:
            logger.error(f"Ошибка запуска benchmark: {e}")
            import traceback
            traceback.print_exc()
            # Не падаем - веб-интерфейс будет работать, показывая нули


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan события для FastAPI"""
    # Startup
    try:
        await init_benchmark()
    except Exception as e:
        logger.error(f"Ошибка в lifespan: {e}")
    yield
    # Shutdown

app = FastAPI(title="pyjobkit Load Test Dashboard", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    rate, concurrency = load_settings()
    try:
        queue_length = max(metrics.enqueued - metrics.processed, 0)
        runtime_s = time.time() - metrics.start_time
        stalled = runtime_s > 5 and metrics.processed == 0
        
        # Получаем счётчик Redis для проверки
        redis_counter = await get_redis_counter()
        
        # Debug: получаем счётчики INCRBY (импорт в начале файла)
        import memory_db
        debug_incrby_count = memory_db._debug_incrby_count
        debug_incrby_total = memory_db._debug_incrby_total
        
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "current_rps": metrics.rps_history[-1] if metrics.rps_history else 0,
                "avg_rps": sum(metrics.rps_history) / len(metrics.rps_history)
                if metrics.rps_history
                else 0,
                "total_processed": metrics.processed,
                "redis_counter": redis_counter,
                "debug_incrby_count": debug_incrby_count,
                "debug_incrby_total": debug_incrby_total,
                "queue_length": queue_length,
                "rps_history": list(metrics.rps_history),
                "cpu_history": list(metrics.cpu_history) if hasattr(metrics, 'cpu_history') else [],
                "ram_history": list(metrics.ram_history) if hasattr(metrics, 'ram_history') else [],
                "rps_per_minute": list(metrics.rps_per_minute) if hasattr(metrics, 'rps_per_minute') else [],
                "cpu_per_minute": list(metrics.cpu_per_minute) if hasattr(metrics, 'cpu_per_minute') else [],
                "ram_per_minute": list(metrics.ram_per_minute) if hasattr(metrics, 'ram_per_minute') else [],
                "current_cpu": metrics.cpu_history[-1] if hasattr(metrics, 'cpu_history') and metrics.cpu_history else 0,
                "current_ram": metrics.ram_history[-1] if hasattr(metrics, 'ram_history') and metrics.ram_history else 0,
                "running_dict_size": getattr(metrics, 'running_dict_size', 0),
                "uptime_minutes": len(metrics.rps_per_minute) if hasattr(metrics, 'rps_per_minute') else 0,
                "stalled": stalled,
                "error_events": metrics.error_events,
                "last_error": metrics.last_error,
                "enqueue_rate": rate,
                "concurrency": concurrency,
            },
        )
    except Exception as e:
        # Если metrics не инициализированы, показываем пустую страницу
        import traceback
        traceback.print_exc()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "current_rps": 0,
                "avg_rps": 0,
                "total_processed": 0,
                "redis_counter": -1,
                "queue_length": 0,
                "rps_history": [],
                "cpu_history": [],
                "ram_history": [],
                "current_cpu": 0,
                "current_ram": 0,
                "stalled": False,
                "error_events": 0,
                "last_error": f"Ошибка: {str(e)}",
                "enqueue_rate": rate,
                "concurrency": concurrency,
            },
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
