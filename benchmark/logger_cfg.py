import logging
import os

logger = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()  # Отключаем логи для производительности

def configure_logging() -> None:
    level = getattr(logging, LOG_LEVEL, logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logging.getLogger("pyjobkit").setLevel(level)
    logging.getLogger("pyjobkit.backends").setLevel(level)

    local_logger = logging.getLogger(__name__)
    local_logger.debug(
        "Логирование инициализировано: уровень=%s", logging.getLevelName(level)
    )
