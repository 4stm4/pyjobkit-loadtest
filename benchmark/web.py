import asyncio
import os

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

app = FastAPI(title="pyjobkit Load Test Dashboard")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def startup_event():
    dsn, rate, concurrency = load_settings()

    app.state.rate = rate
    app.state.concurrency = concurrency
    app.state.engine, app.state.tasks = await start_benchmark(dsn, rate, concurrency)


@app.on_event("shutdown")
async def shutdown_event():
    tasks = getattr(app.state, "tasks", ())
    if tasks:
        await stop_tasks(tasks)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    queue_length = max(metrics.enqueued - metrics.processed, 0)
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
        },
    )


# Автозапуск бенчмарка + веб-сервера
if __name__ == "__main__":
    async def run_all():
        # Запускаем веб-сервер
        config = uvicorn.Config(app, host="0.0.0.0", port=7777, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(run_all())
