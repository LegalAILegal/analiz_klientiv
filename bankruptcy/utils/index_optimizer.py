import os
import time
import logging
from datetime import datetime, timedelta
from django.conf import settings
from django.core.management import call_command
from django.db import connection
from django.utils import timezone

logger = logging.getLogger(__name__)


class IndexOptimizer:
    """
    Система автоматичної оптимізації індексів для швидкого пошуку
    Інтегрується з існуючими службами моніторингу
    """
    
    def __init__(self):
        self.optimization_log_file = os.path.join(
            settings.BASE_DIR, "logs", "index_optimization.log"
        )
        self.min_records_threshold = getattr(settings, "INDEX_OPTIMIZATION_MIN_RECORDS", 1000)
        self.optimization_interval_hours = getattr(settings, "INDEX_OPTIMIZATION_INTERVAL_HOURS", 24)
        
        # Створюємо директорію для логів
        os.makedirs(os.path.dirname(self.optimization_log_file), exist_ok=True)
    
    def should_optimize_table(self, table_name, records_imported=0):
        """
        Визначає чи потрібно оптимізувати індекси для таблиці
        """
        # Перевіряємо кількість імпортованих записів
        if records_imported < self.min_records_threshold:
            logger.info(f"Пропуск оптимізації {table_name}: імпортовано тільки {records_imported} записів")
            return False
        
        # Перевіряємо час останньої оптимізації
        last_optimization = self.get_last_optimization_time(table_name)
        if last_optimization:
            time_since_last = timezone.now() - last_optimization
            if time_since_last < timedelta(hours=self.optimization_interval_hours):
                logger.info(f"Пропуск оптимізації {table_name}: остання оптимізація {time_since_last} тому")
                return False
        
        return True
    
    def optimize_table_indexes(self, table_name, year=None, records_imported=0):
        """
        Оптимізує індекси для конкретної таблиці
        """
        if not self.should_optimize_table(table_name, records_imported):
            return False
        
        try:
            logger.info(f"🚀 Початок автоматичної оптимізації індексів для {table_name}")
            
            start_time = time.time()
            
            # Запускаємо команду оптимізації індексів
            if year:
                call_command("optimize_court_indexes", year=year, analyze=True, verbosity=0)
            else:
                # Витягуємо рік з назви таблиці
                if "_" in table_name:
                    try:
                        extracted_year = int(table_name.split("_")[-1])
                        call_command("optimize_court_indexes", year=extracted_year, analyze=True, verbosity=0)
                    except (ValueError, IndexError):
                        call_command("optimize_court_indexes", analyze=True, verbosity=0)
                else:
                    call_command("optimize_court_indexes", analyze=True, verbosity=0)
            
            end_time = time.time()
            optimization_time = end_time - start_time
            
            # Записуємо результат оптимізації
            self.log_optimization(table_name, records_imported, optimization_time, True)
            
            logger.info(f"✅ Оптимізація {table_name} завершена за {optimization_time:.2f}с")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка оптимізації індексів для {table_name}: {e}")
            self.log_optimization(table_name, records_imported, 0, False, str(e))
            return False
    
    def optimize_after_import(self, year, records_imported):
        """
        Оптимізує індекси після імпорту судових рішень
        """
        table_name = f"court_decisions_{year}"
        return self.optimize_table_indexes(table_name, year, records_imported)
    
    def get_last_optimization_time(self, table_name):
        """
        Отримує час останньої оптимізації для таблиці
        """
        try:
            if not os.path.exists(self.optimization_log_file):
                return None
            
            with open(self.optimization_log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            # Шукаємо останню успішну оптимізацію для таблиці
            for line in reversed(lines):
                if table_name in line and "SUCCESS" in line:
                    try:
                        # Формат: 2024-01-01 12:00:00 | SUCCESS | table_name | ...
                        timestamp_str = line.split(" | ")[0]
                        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    except (ValueError, IndexError):
                        continue
            
            return None
            
        except Exception as e:
            logger.error(f"Помилка зчитування лог-файлу оптимізації: {e}")
            return None
    
    def log_optimization(self, table_name, records_imported, optimization_time, success, error_msg=None):
        """
        Записує результат оптимізації в лог-файл
        """
        try:
            timestamp = timezone.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "SUCCESS" if success else "FAILED"
            
            log_entry = f"{timestamp} | {status} | {table_name} | {records_imported} records | {optimization_time:.2f}s"
            
            if error_msg:
                log_entry += f" | ERROR: {error_msg}"
            
            log_entry += "\n"
            
            with open(self.optimization_log_file, "a", encoding="utf-8") as f:
                f.write(log_entry)
                
        except Exception as e:
            logger.error(f"Помилка запису лог-файлу оптимізації: {e}")
    
    def get_optimization_statistics(self):
        """
        Отримує статистику оптимізацій
        """
        stats = {
            "total_optimizations": 0,
            "successful_optimizations": 0,
            "failed_optimizations": 0,
            "last_optimization": None,
            "tables_optimized": set()
        }
        
        try:
            if not os.path.exists(self.optimization_log_file):
                return stats
            
            with open(self.optimization_log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            for line in lines:
                if " | SUCCESS | " in line or " | FAILED | " in line:
                    stats["total_optimizations"] += 1
                    
                    parts = line.split(" | ")
                    if len(parts) >= 3:
                        timestamp_str = parts[0]
                        status = parts[1]
                        table_name = parts[2]
                        
                        if status == "SUCCESS":
                            stats["successful_optimizations"] += 1
                        else:
                            stats["failed_optimizations"] += 1
                        
                        stats["tables_optimized"].add(table_name)
                        
                        try:
                            opt_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                            if not stats["last_optimization"] or opt_time > stats["last_optimization"]:
                                stats["last_optimization"] = opt_time
                        except ValueError:
                            pass
            
            stats["tables_optimized"] = list(stats["tables_optimized"])
            
        except Exception as e:
            logger.error(f"Помилка отримання статистики оптимізації: {e}")
        
        return stats
    
    def cleanup_old_logs(self, days_to_keep=30):
        """
        Очищає старі записи з лог-файлу оптимізації
        """
        try:
            if not os.path.exists(self.optimization_log_file):
                return
            
            cutoff_date = timezone.now() - timedelta(days=days_to_keep)
            
            with open(self.optimization_log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            filtered_lines = []
            for line in lines:
                try:
                    timestamp_str = line.split(" | ")[0]
                    log_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    
                    if log_time >= cutoff_date:
                        filtered_lines.append(line)
                except (ValueError, IndexError):
                    # Зберігаємо рядки з некоректним форматом
                    filtered_lines.append(line)
            
            if len(filtered_lines) < len(lines):
                with open(self.optimization_log_file, "w", encoding="utf-8") as f:
                    f.writelines(filtered_lines)
                
                logger.info(f"Очищено {len(lines) - len(filtered_lines)} старих записів з лог-файлу оптимізації")
        
        except Exception as e:
            logger.error(f"Помилка очищення лог-файлу оптимізації: {e}")


# Глобальний екземпляр оптимізатора для використання в службах
index_optimizer = IndexOptimizer()