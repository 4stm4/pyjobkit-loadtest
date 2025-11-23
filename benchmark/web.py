import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
from app import metrics, main  # импортируем метрики и основной цикл

app = FastAPI(title="pyjobkit Load Test Dashboard")
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "current_rps": metrics.rps_history[-1] if metrics.rps_history else 0,
        "avg_rps": sum(metrics.rps_history) / len(metrics.rps_history) if metrics.rps_history else 0,
        "total_processed": metrics.processed,
        "queue_length": len(metrics.rps_history),
        "rps_history": list(metrics.rps_history)
    })

# Автозапуск бенчмарка + веб-сервера
if __name__ == "__main__":
    async def run_all():
        # Запускаем основной цикл бенчмарка в фоне
        asyncio.create_task(main())
        # Запускаем веб-сервер
        config = uvicorn.Config(app, host="0.0.0.0", port=7777, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(run_all())
