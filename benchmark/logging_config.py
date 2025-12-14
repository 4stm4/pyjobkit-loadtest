import logging

def setup_logging():
    """Настройка логирования для более чистого вывода"""
    
    # Устанавливаем базовый уровень
    logging.getLogger().setLevel(logging.INFO)
    
    # Отключаем DEBUG логи от aiosqlite
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    
    # Отключаем DEBUG логи от sqlalchemy
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    
    # Отключаем излишнюю болтливость от uvicorn.access
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    
    # Отключаем DEBUG логи от pyjobkit.backends.sql.backend
    logging.getLogger("pyjobkit.backends.sql.backend").setLevel(logging.INFO)
    
    # Настраиваем формат логов для более читаемого вывода
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Применяем формат к консольному обработчику
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(formatter)