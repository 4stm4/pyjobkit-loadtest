import asyncio
import time

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
from logger_cfg import configure_logging

configure_logging()
templates = Jinja2Templates(directory="templates")

# Глобальные переменные для benchmark
benchmark_task = None
engine = None

app = FastAPI(title="pyjobkit Load Test Dashboard")

async def init_benchmark():
    """Инициализация benchmark с memory backend"""
    global benchmark_task, engine
    
    if benchmark_task is None:
        rate, concurrency = load_settings()
        
        try:
            engine, tasks = await start_benchmark(rate, concurrency)
            # Создаем единую фоновую задачу
            benchmark_task = asyncio.create_task(
                asyncio.gather(*tasks, return_exceptions=True)
            )
            print(f"✓ Загрузочное тестирование запущено (мемори): {rate} RPS с {concurrency} воркерами")
        except Exception as e:
            print(f"✗ Ошибка запуска benchmark: {e}")

@app.on_event("startup")
async def startup_event():
    """Запуск benchmark при старте приложения"""
    await init_benchmark()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        queue_length = max(metrics.enqueued - metrics.processed, 0)
        runtime_s = time.time() - metrics.start_time
        stalled = runtime_s > 5 and metrics.processed == 0
        
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "current_rps": metrics.rps_history[-1] if metrics.rps_history else 0,
                "avg_rps": sum(metrics.rps_history) / len(metrics.rps_history)
                if metrics.rps_history
                else 0,
                "total_processed": metrics.processed,
                "queue_length": queue_length,
                "rps_history": list(metrics.rps_history),
                "stalled": stalled,
                "error_events": metrics.error_events,
                "last_error": metrics.last_error,
            },
        )
    except Exception as e:
        # Если metrics не инициализированы, показываем пустую страницу
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "current_rps": 0,
                "avg_rps": 0,
                "total_processed": 0,
                "queue_length": 0,
                "rps_history": [],
                "stalled": False,
                "error_events": 0,
                "last_error": f"Сервис запускается... ({str(e)})",
            },
        )

if __name__ == "__main__":
    print("🚀 Запуск с memory backend")
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="info")
