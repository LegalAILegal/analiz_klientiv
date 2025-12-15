"""
Декоратори для безпечної роботи з базою даних
"""
import functools
import logging
from django.db import transaction
from bankruptcy.utils.connection_manager import safe_db_connection, limited_thread_execution

logger = logging.getLogger(__name__)

def safe_db_operation(func):
    """
    Декоратор для безпечного виконання операцій з базою даних
    Автоматично управляє з"єднаннями та транзакціями
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with limited_thread_execution():
                with safe_db_connection() as connection:
                    return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Помилка в операції з БД {func.__name__}: {e}")
            raise
    return wrapper

def atomic_db_operation(func):
    """
    Декоратор для безпечного виконання атомарних операцій з базою даних
    Поєднує обмеження з"єднань з транзакціями
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with limited_thread_execution():
                with transaction.atomic():
                    return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Помилка в атомарній операції з БД {func.__name__}: {e}")
            raise
    return wrapper

def bulk_db_operation(batch_size=100):
    """
    Декоратор для безпечного виконання масових операцій з базою даних
    Автоматично розбиває операції на батчі
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(items, *args, **kwargs):
            results = []
            try:
                with limited_thread_execution():
                    # Розбиваємо items на батчі
                    for i in range(0, len(items), batch_size):
                        batch = items[i:i + batch_size]
                        logger.debug(f"Обробка батчу {i//batch_size + 1}: {len(batch)} елементів")
                        
                        with safe_db_connection() as connection:
                            batch_result = func(batch, *args, **kwargs)
                            results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
                        
                        # Очищаємо з"єднання між батчами
                        from bankruptcy.utils.connection_manager import cleanup_connections
                        cleanup_connections()
                
            except Exception as e:
                logger.error(f"Помилка в масовій операції з БД {func.__name__}: {e}")
                raise
            
            return results
        return wrapper
    return decorator

def monitor_db_connections(func):
    """
    Декоратор для моніторингу з"єднань до бази даних
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        from bankruptcy.utils.connection_manager import get_connection_status
        
        status_before = get_connection_status()
        logger.info(f"Початок {func.__name__}: активних з"єднань {status_before["active_connections"]}/{status_before["max_connections"]}")
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            status_after = get_connection_status()
            logger.info(f"Кінець {func.__name__}: активних з"єднань {status_after["active_connections"]}/{status_after["max_connections"]}")
            
            # Попередження якщо занадто багато активних з"єднань
            if status_after["active_connections"] > status_after["max_connections"] * 0.8:
                logger.warning(f"Високе використання з"єднань: {status_after["active_connections"]}/{status_after["max_connections"]}")
    
    return wrapper