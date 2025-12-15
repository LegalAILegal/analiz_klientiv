import os
import time
import threading
from datetime import datetime, timedelta
from django.core.management import call_command
from django.conf import settings
from django.utils import timezone
import logging
from bankruptcy.utils.fast_court_search import get_search_statistics
from bankruptcy.utils.fast_resolution_extractor import get_extraction_statistics

logger = logging.getLogger(__name__)


class FastMonitorService:
    """
    Високопродуктивний сервіс моніторингу та автоматичної обробки 
    на основі алгоритмів SR_AI
    """
    
    def __init__(self):
        self.is_running = False
        self.search_thread = None
        self.extraction_thread = None
        self.stop_event = threading.Event()
        
        # Налаштування
        self.search_interval = 60  # секунд між циклами пошуку
        self.extraction_interval = 90  # секунд між циклами витягування
        self.batch_size_search = 600
        self.batch_size_extraction = 300
        
        # Статистика
        self.last_search_cycle = None
        self.last_extraction_cycle = None
        self.total_search_cycles = 0
        self.total_extraction_cycles = 0
        self.total_found_decisions = 0
        self.total_extracted_resolutions = 0
        
    def start(self):
        """Запускає сервіс моніторингу"""
        if self.is_running:
            logger.warning("Сервіс моніторингу вже працює")
            return
        
        logger.info("Запуск швидкого сервісу моніторингу...")
        
        self.is_running = True
        self.stop_event.clear()
        
        # Запускаємо окремі потоки для пошуку та витягування
        self.search_thread = threading.Thread(target=self._continuous_search, daemon=True)
        self.extraction_thread = threading.Thread(target=self._continuous_extraction, daemon=True)
        
        self.search_thread.start()
        self.extraction_thread.start()
        
        logger.info("Швидкий сервіс моніторингу запущено")
    
    def stop(self):
        """Зупиняє сервіс моніторингу"""
        if not self.is_running:
            logger.warning("Сервіс моніторингу не працює")
            return
        
        logger.info("Зупинка швидкого сервісу моніторингу...")
        
        self.is_running = False
        self.stop_event.set()
        
        # Чекаємо завершення потоків
        if self.search_thread and self.search_thread.is_alive():
            self.search_thread.join(timeout=10)
        
        if self.extraction_thread and self.extraction_thread.is_alive():
            self.extraction_thread.join(timeout=10)
        
        logger.info("Швидкий сервіс моніторингу зупинено")
    
    def _continuous_search(self):
        """Безперервний пошук судових рішень"""
        logger.info("Запуск безперервного пошуку судових рішень...")
        
        while not self.stop_event.is_set():
            try:
                cycle_start_time = time.time()
                self.total_search_cycles += 1
                
                logger.info(f"=== ЦИКЛ ПОШУКУ {self.total_search_cycles} ===")
                
                # Отримуємо статистику
                stats = get_search_statistics()
                
                logger.info(
                    f"Статистика пошуку: {stats["cases_with_decisions"]}/{stats["total_cases"]} "
                    f"справ мають рішення ({stats["search_percentage"]:.1f}%)"
                )
                
                if stats["pending_cases"] == 0:
                    logger.info("Всі справи оброблено. Очікування нових справ...")
                    time.sleep(60)
                    continue
                
                logger.info(f"Знайдено {stats["pending_cases"]} справ для пошуку")
                
                # Виконуємо пошук швидким сервісом
                try:
                    call_command("search_court_decisions_fast", limit=self.batch_size_search)
                    
                    # Оновлюємо статистику
                    new_stats = get_search_statistics()
                    found_in_cycle = new_stats["total_decisions"] - stats["total_decisions"]
                    self.total_found_decisions += found_in_cycle
                    
                    if found_in_cycle > 0:
                        logger.info(f"Знайдено {found_in_cycle} нових рішень в циклі")
                
                except Exception as e:
                    logger.error(f"Помилка в циклі пошуку: {e}")
                
                cycle_duration = time.time() - cycle_start_time
                self.last_search_cycle = timezone.now()
                
                logger.info(
                    f"Цикл пошуку {self.total_search_cycles} завершено за {cycle_duration:.2f}с"
                )
                
                # Очищаємо з"єднання після циклу через менеджер з"єднань
                from bankruptcy.utils.connection_manager import cleanup_connections
                cleanup_connections()
                
                # Пауза між циклами
                if not self.stop_event.wait(self.search_interval):
                    continue
                else:
                    break
                
            except Exception as e:
                logger.error(f"Критична помилка в безперервному пошуку: {e}")
                time.sleep(30)
    
    def _continuous_extraction(self):
        """Безперервне витягування резолютивних частин"""
        logger.info("Запуск безперервного витягування резолютивних частин...")
        
        while not self.stop_event.is_set():
            try:
                cycle_start_time = time.time()
                self.total_extraction_cycles += 1
                
                logger.info(f"=== ЦИКЛ ВИТЯГУВАННЯ {self.total_extraction_cycles} ===")
                
                # Отримуємо статистику
                stats = get_extraction_statistics()
                
                logger.info(
                    f"Статистика витягування: {stats["extracted_decisions"]}/{stats["total_decisions"]} "
                    f"рішень мають резолютивні частини ({stats["extraction_percentage"]:.1f}%)"
                )
                
                if stats["pending_decisions"] == 0:
                    logger.info("Всі резолютивні частини витягнуто. Очікування нових рішень...")
                    time.sleep(60)
                    continue
                
                logger.info(f"Знайдено {stats["pending_decisions"]} рішень для витягування")
                
                # Виконуємо витягування швидким сервісом
                try:
                    call_command("extract_resolutions_fast", limit=self.batch_size_extraction)
                    
                    # Оновлюємо статистику
                    new_stats = get_extraction_statistics()
                    extracted_in_cycle = new_stats["extracted_decisions"] - stats["extracted_decisions"]
                    self.total_extracted_resolutions += extracted_in_cycle
                    
                    if extracted_in_cycle > 0:
                        logger.info(f"Витягнуто {extracted_in_cycle} резолютивних частин в циклі")
                
                except Exception as e:
                    logger.error(f"Помилка в циклі витягування: {e}")
                
                cycle_duration = time.time() - cycle_start_time
                self.last_extraction_cycle = timezone.now()
                
                logger.info(
                    f"Цикл витягування {self.total_extraction_cycles} завершено за {cycle_duration:.2f}с"
                )
                
                # Очищаємо з"єднання після циклу через менеджер з"єднань
                from bankruptcy.utils.connection_manager import cleanup_connections
                cleanup_connections()
                
                # Пауза між циклами
                if not self.stop_event.wait(self.extraction_interval):
                    continue
                else:
                    break
                
            except Exception as e:
                logger.error(f"Критична помилка в безперервному витягуванні: {e}")
                time.sleep(30)
    
    def get_status(self):
        """Повертає статус сервісу моніторингу"""
        search_stats = get_search_statistics()
        extraction_stats = get_extraction_statistics()
        
        return {
            "is_running": self.is_running,
            "start_time": getattr(self, "start_time", None),
            "last_search_cycle": self.last_search_cycle,
            "last_extraction_cycle": self.last_extraction_cycle,
            "total_search_cycles": self.total_search_cycles,
            "total_extraction_cycles": self.total_extraction_cycles,
            "total_found_decisions": self.total_found_decisions,
            "total_extracted_resolutions": self.total_extracted_resolutions,
            "search_stats": search_stats,
            "extraction_stats": extraction_stats,
            "threads": {
                "search_alive": self.search_thread.is_alive() if self.search_thread else False,
                "extraction_alive": self.extraction_thread.is_alive() if self.extraction_thread else False
            }
        }
    
    def process_new_court_decisions(self, year=None):
        """
        Обробляє нові судові рішення після їх імпорту
        """
        try:
            logger.info(f"Початок обробки нових судових рішень за {year} рік...")
            
            # Запускаємо пошук для справ без рішень
            search_stats_before = get_search_statistics()
            call_command("search_court_decisions_fast", limit=self.batch_size_search)
            search_stats_after = get_search_statistics()
            
            new_decisions = search_stats_after["total_decisions"] - search_stats_before["total_decisions"]
            
            if new_decisions > 0:
                logger.info(f"Знайдено {new_decisions} нових рішень")
                
                # Запускаємо витягування резолютивних частин для нових рішень
                extraction_stats_before = get_extraction_statistics()
                call_command("extract_resolutions_fast", limit=new_decisions)
                extraction_stats_after = get_extraction_statistics()
                
                new_extractions = extraction_stats_after["extracted_decisions"] - extraction_stats_before["extracted_decisions"]
                logger.info(f"Витягнуто {new_extractions} резолютивних частин")
            
            logger.info("Обробка нових судових рішень завершена")
            
        except Exception as e:
            logger.error(f"Помилка при обробці нових судових рішень: {e}")
    
    def monitor_database_changes(self):
        """
        Моніторить зміни в базах даних судових рішень
        """
        from django.db import connection
        
        while not self.stop_event.is_set():
            try:
                # Перевіряємо чи з"явились нові судові рішення за останні 5 хвилин
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM bankruptcy_trackedcourtdecision 
                        WHERE created_at >= %s
                    """, [timezone.now() - timedelta(minutes=5)])
                    
                    new_decisions_count = cursor.fetchone()[0]
                    
                    if new_decisions_count > 0:
                        logger.info(f"Виявлено {new_decisions_count} нових судових рішень. Запуск обробки...")
                        self.process_new_court_decisions()
                
                # Перевіряємо кожні 5 хвилин
                if self.stop_event.wait(300):  # 5 хвилин = 300 секунд
                    break
                    
            except Exception as e:
                logger.error(f"Помилка моніторингу змін в базі даних: {e}")
                time.sleep(60)


# Глобальний екземпляр сервісу
_monitor_service = None


def get_monitor_service():
    """Повертає глобальний екземпляр сервісу моніторингу"""
    global _monitor_service
    if _monitor_service is None:
        _monitor_service = FastMonitorService()
    return _monitor_service


def start_fast_monitoring():
    """Запускає швидкий сервіс моніторингу"""
    service = get_monitor_service()
    service.start()
    return service


def stop_fast_monitoring():
    """Зупиняє швидкий сервіс моніторингу"""
    service = get_monitor_service()
    service.stop()
    return service


def get_monitoring_status():
    """Повертає статус швидкого сервісу моніторингу"""
    service = get_monitor_service()
    return service.get_status()


def trigger_court_decisions_processing(year=None):
    """Тригерить обробку нових судових рішень"""
    service = get_monitor_service()
    service.process_new_court_decisions(year)