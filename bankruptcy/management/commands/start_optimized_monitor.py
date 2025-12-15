"""
Оптимізований файловий моніторинг з мінімальним навантаженням
Запускається тільки при реальних змінах файлів та працює в економному режимі
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.conf import settings
import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
from bankruptcy.models import SystemProcessControl
from datetime import datetime, timedelta

# Налаштування логування
logger = logging.getLogger("optimized_monitor")

class OptimizedCSVHandler(FileSystemEventHandler):
    """
    Оптимізований обробник подій файлової системи
    """
    
    def __init__(self, command_instance):
        self.command = command_instance
        self.file_hashes = {}
        self.last_check = {}
        self.cooldown_period = 300  # 5 хвилин кулдауну між обробками
        
        # Обчислюємо початкові хеші файлів
        self.update_file_hashes()
        
    def update_file_hashes(self):
        """Оновлює хеші всіх відстежуваних файлів"""
        files_to_monitor = [
            "data/Відомості про справи про банкрутство.csv",
            "data/documents_19.csv", "data/documents_20.csv", "data/documents_21.csv",
            "data/documents_22.csv", "data/documents_23.csv", "data/documents_24.csv", "data/documents_25.csv"
        ]
        
        for file_path in files_to_monitor:
            if os.path.exists(file_path):
                try:
                    with open(file_path, "rb") as f:
                        # Читаємо тільки перші та останні 8KB для швидкого хешування
                        start_chunk = f.read(8192)
                        f.seek(-8192, 2)
                        end_chunk = f.read(8192)
                        
                    file_hash = hashlib.md5(start_chunk + end_chunk).hexdigest()
                    file_size = os.path.getsize(file_path)
                    file_mtime = os.path.getmtime(file_path)
                    
                    self.file_hashes[file_path] = {
                        "hash": file_hash,
                        "size": file_size,
                        "mtime": file_mtime
                    }
                except Exception as e:
                    logger.error(f"Помилка читання файлу {file_path}: {e}")
    
    def on_modified(self, event):
        """Обробляє подію зміни файлу"""
        if event.is_directory:
            return
            
        file_path = event.src_path
        
        # Перевіряємо тільки CSV файли
        if not file_path.endswith(".csv"):
            return
            
        # Кулдаун - не обробляємо файл якщо він недавно оброблявся
        now = time.time()
        if file_path in self.last_check:
            if now - self.last_check[file_path] < self.cooldown_period:
                return
        
        self.last_check[file_path] = now
        
        # Перевіряємо чи дійсно змінився файл
        if self.has_file_really_changed(file_path):
            logger.info(f"📁 РЕАЛЬНА ЗМІНА ФАЙЛУ: {file_path}")
            self.command.process_file_change(file_path)
            self.update_file_hashes()
        else:
            logger.debug(f"📁 Ложна тривога для файлу: {file_path}")
    
    def has_file_really_changed(self, file_path):
        """Перевіряє чи дійсно змінився файл (не просто час доступу)"""
        if not os.path.exists(file_path):
            return False
            
        try:
            current_size = os.path.getsize(file_path)
            current_mtime = os.path.getmtime(file_path)
            
            # Швидка перевірка за розміром та часом модифікації
            if file_path in self.file_hashes:
                old_data = self.file_hashes[file_path]
                if old_data["size"] == current_size and abs(old_data["mtime"] - current_mtime) < 1:
                    return False
            
            # Детальна перевірка хешу (тільки якщо розмір або час змінився)
            with open(file_path, "rb") as f:
                start_chunk = f.read(8192)
                f.seek(-8192, 2)
                end_chunk = f.read(8192)
                
            current_hash = hashlib.md5(start_chunk + end_chunk).hexdigest()
            
            if file_path in self.file_hashes:
                return current_hash != self.file_hashes[file_path]["hash"]
            
            return True
            
        except Exception as e:
            logger.error(f"Помилка перевірки файлу {file_path}: {e}")
            return False


class Command(BaseCommand):
    help = "Оптимізований моніторинг файлів з мінімальним навантаженням"

    def add_arguments(self, parser):
        parser.add_argument(
            "--check-interval",
            type=int,
            default=600,  # 10 хвилин замість постійного моніторингу
            help="Інтервал перевірки файлів (секунди)"
        )
        parser.add_argument(
            "--minimal-mode",
            action="store_true",
            help="Мінімальний режим - тільки перевірка без активної обробки"
        )

    def handle(self, *args, **options):
        self.check_interval = options["check_interval"]
        self.minimal_mode = options["minimal_mode"]
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🔧 ЗАПУСК ОПТИМІЗОВАНОГО МОНІТОРИНГУ\n"
                f"   - Інтервал перевірки: {self.check_interval} секунд\n"
                f"   - Мінімальний режим: {"ТАК" if self.minimal_mode else "НІ"}"
            )
        )
        
        # Оновлюємо статус процесу
        try:
            process_control, created = SystemProcessControl.objects.get_or_create(
                process_type="file_monitoring",
                defaults={
                    "status": "running",
                    "started_at": timezone.now(),
                    "last_message": "🔧 Оптимізований моніторинг запущено"
                }
            )
            if not created:
                process_control.status = "running"
                process_control.started_at = timezone.now()
                process_control.last_message = "🔧 Оптимізований моніторинг запущено"
                process_control.save()
        except Exception as e:
            self.stdout.write(f"⚠️ Помилка оновлення статусу: {e}")
        
        # Запускаємо моніторинг
        if self.minimal_mode:
            self.run_minimal_monitoring()
        else:
            self.run_watchdog_monitoring()
    
    def run_minimal_monitoring(self):
        """Мінімальний режим - перевірка за розкладом"""
        handler = OptimizedCSVHandler(self)
        
        while True:
            try:
                # Оновлюємо статус
                process_control = SystemProcessControl.objects.get(process_type="file_monitoring")
                process_control.last_message = f"🔧 Мінімальна перевірка: {datetime.now().strftime("%H:%M:%S")}"
                process_control.save()
                
                # Перевіряємо всі файли
                handler.update_file_hashes()
                
                self.stdout.write(f"🔍 Перевірка завершена: {datetime.now().strftime("%H:%M:%S")}")
                
                # Чекаємо до наступної перевірки
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                self.stdout.write(self.style.WARNING("⏹️ Моніторинг зупинено користувачем"))
                break
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Помилка моніторингу: {e}"))
                time.sleep(60)  # Чекаємо хвилину при помилці
    
    def run_watchdog_monitoring(self):
        """Стандартний режим з watchdog"""
        observer = Observer()
        event_handler = OptimizedCSVHandler(self)
        
        # Додаємо спостерігача для директорії data
        data_path = os.path.join(settings.BASE_DIR, "data")
        if os.path.exists(data_path):
            observer.schedule(event_handler, data_path, recursive=False)
            self.stdout.write(f"👁️ Спостерігаю за директорією: {data_path}")
        
        observer.start()
        
        try:
            while True:
                time.sleep(self.check_interval)
                
                # Періодичне оновлення статусу
                try:
                    process_control = SystemProcessControl.objects.get(process_type="file_monitoring")
                    process_control.last_message = f"🔧 Оптимізований моніторинг активний: {datetime.now().strftime("%H:%M:%S")}"
                    process_control.save()
                except:
                    pass
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("⏹️ Моніторинг зупинено користувачем"))
        finally:
            observer.stop()
            observer.join()
    
    def process_file_change(self, file_path):
        """Обробляє зміну файлу"""
        if self.minimal_mode:
            self.stdout.write(f"📁 Файл змінено (мінімальний режим): {file_path}")
            return
        
        self.stdout.write(f"📁 Обробляю зміну файлу: {file_path}")
        
        # Тут можна додати логіку обробки конкретних файлів
        if "bankruptcy" in file_path:
            self.stdout.write("💼 Файл справ банкрутства змінено")
        elif "documents_" in file_path:
            year = file_path.split("documents_")[1].split(".")[0]
            self.stdout.write(f"📋 Файл судових рішень {year} року змінено")
        
        # Оновлюємо статус процесу
        try:
            process_control = SystemProcessControl.objects.get(process_type="file_monitoring")
            process_control.last_message = f"📁 Оброблено зміну: {os.path.basename(file_path)} ({datetime.now().strftime("%H:%M:%S")})"
            process_control.save()
        except:
            pass