"""
Management command для моніторингу змін у CSV файлах судових рішень
та автоматичного оновлення кешу
"""

from django.core.management.base import BaseCommand
from bankruptcy.models import CourtDecisionStatistics
import os
import time
import glob
from datetime import datetime


class Command(BaseCommand):
    help = "Моніторить зміни у CSV файлах судових рішень та інвалідує кеш"

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv-dir",
            type=str,
            default="/home/ruslan/PYTHON/analiz_klientiv/data/",
            help="Директорія з CSV файлами для моніторингу"
        )
        parser.add_argument(
            "--check-interval",
            type=int,
            default=300,  # 5 хвилин
            help="Інтервал перевірки змін (в секундах)"
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Виконати перевірку один раз і вийти"
        )

    def handle(self, *args, **options):
        csv_dir = options["csv_dir"]
        check_interval = options["check_interval"]
        run_once = options["run_once"]

        self.stdout.write(f"🔍 Початок моніторингу CSV файлів в {csv_dir}")
        
        if not os.path.exists(csv_dir):
            self.stdout.write(
                self.style.ERROR(f"❌ Директорія {csv_dir} не існує")
            )
            return

        # Словник для збереження часу останньої модифікації файлів
        last_modified = {}
        
        while True:
            try:
                # Шукаємо всі CSV файли судових рішень
                csv_pattern = os.path.join(csv_dir, "*court_decisions*.csv")
                csv_files = glob.glob(csv_pattern)
                
                if not csv_files:
                    csv_pattern = os.path.join(csv_dir, "documents_*.csv")
                    csv_files = glob.glob(csv_pattern)
                
                changes_detected = False
                
                for csv_file in csv_files:
                    try:
                        current_mtime = os.path.getmtime(csv_file)
                        file_name = os.path.basename(csv_file)
                        
                        if file_name not in last_modified:
                            # Перший раз бачимо цей файл
                            last_modified[file_name] = current_mtime
                            self.stdout.write(f"📄 Зареєстровано файл: {file_name}")
                        
                        elif current_mtime > last_modified[file_name]:
                            # Файл був змінений
                            old_time = datetime.fromtimestamp(last_modified[file_name])
                            new_time = datetime.fromtimestamp(current_mtime)
                            
                            self.stdout.write(
                                f"🔄 Зміна виявлена в {file_name}:\n"
                                f"   Було: {old_time.strftime("%d.%m.%Y %H:%M:%S")}\n"
                                f"   Стало: {new_time.strftime("%d.%m.%Y %H:%M:%S")}"
                            )
                            
                            last_modified[file_name] = current_mtime
                            changes_detected = True
                    
                    except OSError as e:
                        self.stdout.write(
                            self.style.WARNING(f"⚠️ Не можу прочитати файл {csv_file}: {e}")
                        )
                
                # Якщо виявлені зміни, інвалідуємо кеш
                if changes_detected:
                    invalidated_count = CourtDecisionStatistics.objects.filter(
                        is_valid=True
                    ).update(is_valid=False)
                    
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Інвалідовано {invalidated_count} записів кешу через зміни в CSV файлах"
                        )
                    )
                
                if run_once:
                    break
                
                # Чекаємо до наступної перевірки
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                self.stdout.write("\n👋 Зупинка моніторингу за запитом користувача")
                break
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Помилка моніторингу: {e}")
                )
                if run_once:
                    break
                time.sleep(check_interval)