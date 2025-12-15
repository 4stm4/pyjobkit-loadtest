import logging
import os

logger = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()

def configure_logging() -> None:
    """Единая настройка логирования для всего приложения"""
    level = getattr(logging, LOG_LEVEL, logging.ERROR)
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    
    # Отключаем шумные логгеры
    logging.getLogger("pyjobkit").setLevel(level)
    logging.getLogger("pyjobkit.backends").setLevel(logging.CRITICAL)
    logging.getLogger("uvicorn.access").setLevel(logging.ERROR)


# Алиас для обратной совместимости
setup_logging = configure_logging
