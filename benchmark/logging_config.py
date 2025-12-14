import logging

def setup_logging():
    """Настройка логирования для максимально чистого вывода"""
    
    # Устанавливаем базовый уровень только для ERROR
    logging.getLogger().setLevel(logging.ERROR)
    
    # Полностью отключаем все логи от SQL-связанных библиотек (они больше не используются)
    logging.getLogger("aiosqlite").setLevel(logging.CRITICAL)
    logging.getLogger("sqlalchemy").setLevel(logging.CRITICAL)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.CRITICAL)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)
    
    # Отключаем излишнюю болтливость от uvicorn.access
    logging.getLogger("uvicorn.access").setLevel(logging.ERROR)
    
    # Отключаем все логи от pyjobkit (теперь используем memory backend)
    logging.getLogger("pyjobkit").setLevel(logging.ERROR)
    logging.getLogger("pyjobkit.backends").setLevel(logging.CRITICAL)
    logging.getLogger("pyjobkit.backends.sql").setLevel(logging.CRITICAL)
    logging.getLogger("pyjobkit.backends.memory").setLevel(logging.ERROR)
    logging.getLogger("pyjobkit.engine").setLevel(logging.ERROR)
    
    # Настраиваем формат логов для более читаемого вывода
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Применяем формат к консольному обработчику
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(formatter)