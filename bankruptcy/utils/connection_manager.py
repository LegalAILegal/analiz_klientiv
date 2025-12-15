"""
Менеджер з"єднань для оптимального управління з"єднаннями до PostgreSQL
"""
import threading
import logging
from django.db import connections
from django.conf import settings
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Менеджер з"єднань для обмеження кількості одночасних з"єднань до PostgreSQL
    """
    
    def __init__(self):
        # Читаємо налаштування з settings.py
        self.max_total_connections = getattr(settings, "MAX_TOTAL_DB_CONNECTIONS", 50)
        self.max_concurrent_threads = getattr(settings, "MAX_CONCURRENT_THREADS", 10)
        
        # Семафор для обмеження кількості одночасних з"єднань
        self.connection_semaphore = threading.Semaphore(self.max_total_connections)
        self.thread_semaphore = threading.Semaphore(self.max_concurrent_threads)
        
        # Лічильники для моніторингу
        self.active_connections = 0
        self.active_threads = 0
        self.lock = threading.Lock()
        
        logger.info(f"ConnectionManager ініціалізовано: max_connections={self.max_total_connections}, max_threads={self.max_concurrent_threads}")
    
    @contextmanager
    def get_connection(self, alias="default"):
        """
        Контекстний менеджер для безпечного отримання з"єднання до БД
        """
        # Обмежуємо кількість одночасних з"єднань
        self.connection_semaphore.acquire()
        
        with self.lock:
            self.active_connections += 1
            logger.debug(f"Отримано з"єднання: активних з"єднань {self.active_connections}/{self.max_total_connections}")
        
        connection = None
        try:
            connection = connections[alias]
            yield connection
        finally:
            # Закриваємо з"єднання після використання
            if connection:
                connection.close()
            
            with self.lock:
                self.active_connections -= 1
                logger.debug(f"Закрито з"єднання: активних з"єднань {self.active_connections}/{self.max_total_connections}")
            
            self.connection_semaphore.release()
    
    @contextmanager
    def limit_threads(self):
        """
        Контекстний менеджер для обмеження кількості одночасних потоків
        """
        # Обмежуємо кількість одночасних потоків
        self.thread_semaphore.acquire()
        
        with self.lock:
            self.active_threads += 1
            logger.debug(f"Запущено потік: активних потоків {self.active_threads}/{self.max_concurrent_threads}")
        
        try:
            yield
        finally:
            with self.lock:
                self.active_threads -= 1
                logger.debug(f"Завершено потік: активних потоків {self.active_threads}/{self.max_concurrent_threads}")
            
            self.thread_semaphore.release()
    
    def get_status(self):
        """Повертає статус менеджера з"єднань"""
        with self.lock:
            return {
                "active_connections": self.active_connections,
                "max_connections": self.max_total_connections,
                "active_threads": self.active_threads,
                "max_threads": self.max_concurrent_threads,
                "available_connections": self.max_total_connections - self.active_connections,
                "available_threads": self.max_concurrent_threads - self.active_threads
            }
    
    def force_close_all_connections(self):
        """Примусово закриває всі з"єднання до БД"""
        try:
            for connection in connections.all():
                connection.close()
            logger.info("Примусово закрито всі з"єднання до БД")
        except Exception as e:
            logger.error(f"Помилка при закритті з"єднань: {e}")

# Глобальний екземпляр менеджера з"єднань
_connection_manager = None

def get_connection_manager():
    """Повертає глобальний екземпляр менеджера з"єднань"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager

@contextmanager
def safe_db_connection(alias="default"):
    """
    Контекстний менеджер для безпечного використання з"єднання до БД
    
    Використання:
    with safe_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM table")
        results = cursor.fetchall()
    """
    manager = get_connection_manager()
    with manager.get_connection(alias) as connection:
        yield connection

@contextmanager
def limited_thread_execution():
    """
    Контекстний менеджер для обмеження кількості одночасних потоків
    
    Використання:
    with limited_thread_execution():
        # Код що виконується з обмеженням потоків
        some_heavy_operation()
    """
    manager = get_connection_manager()
    with manager.limit_threads():
        yield

def cleanup_connections():
    """Очищає всі з"єднання до БД"""
    manager = get_connection_manager()
    manager.force_close_all_connections()

def get_connection_status():
    """Повертає статус з"єднань"""
    manager = get_connection_manager()
    return manager.get_status()