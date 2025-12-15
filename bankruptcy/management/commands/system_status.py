"""
Команда для перевірки статусу всієї системи після оптимізації
"""

from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.models import *
import os
import psutil
from datetime import datetime

class Command(BaseCommand):
    help = "Показує повний статус оптимізованої системи"

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS("=== 📊 СТАТУС ОПТИМІЗОВАНОЇ СИСТЕМИ ===")
        )
        
        # 1. Статистика даних
        self.show_data_statistics()
        
        # 2. Статус процесів
        self.show_process_status()
        
        # 3. Системні ресурси
        self.show_system_resources()
        
        # 4. Рекомендації
        self.show_recommendations()
    
    def show_data_statistics(self):
        """Показує статистику даних"""
        self.stdout.write("\n📈 СТАТИСТИКА ДАНИХ:")
        
        try:
            # Справи
            total_cases = BankruptcyCase.objects.count()
            tracked_cases = TrackedBankruptcyCase.objects.count()
            self.stdout.write(f"  📋 Справи банкрутства: {total_cases:,}")
            self.stdout.write(f"  📍 На відстеженні: {tracked_cases:,}")
            
            # Судові рішення
            total_decisions = TrackedCourtDecision.objects.count()
            with_resolution = TrackedCourtDecision.objects.exclude(
                resolution_text__isnull=True
            ).exclude(resolution_text="").count()
            
            self.stdout.write(f"  ⚖️ Судові рішення: {total_decisions:,}")
            self.stdout.write(f"  📝 З резолютивними частинами: {with_resolution:,} ({with_resolution/total_decisions*100:.1f}%)")
            
            # Бази даних судових рішень
            self.stdout.write(f"  📊 Бази даних по роках:")
            for year in range(2019, 2026):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SELECT COUNT(*) FROM court_decisions_{year}")
                        count = cursor.fetchone()[0]
                        self.stdout.write(f"    • {year}: {count:,} рішень")
                except:
                    pass
                    
        except Exception as e:
            self.stdout.write(f"  ❌ Помилка: {e}")
    
    def show_process_status(self):
        """Показує статус процесів"""
        self.stdout.write("\n🔧 СТАТУС ПРОЦЕСІВ:")
        
        try:
            # Системні процеси управління
            processes = SystemProcessControl.objects.all()
            for process in processes:
                status_icon = {
                    "idle": "😴",
                    "running": "🔄",
                    "completed": "✅",
                    "failed": "❌",
                    "stopped": "⏹️"
                }.get(process.status, "❓")
                
                self.stdout.write(
                    f"  {status_icon} {process.get_process_type_display()}: "
                    f"{process.get_status_display()}"
                )
                if process.last_message:
                    self.stdout.write(f"    📝 {process.last_message[:100]}...")
        except Exception as e:
            self.stdout.write(f"  ❌ Помилка: {e}")
    
    def show_system_resources(self):
        """Показує використання системних ресурсів"""
        self.stdout.write("\n💻 СИСТЕМНІ РЕСУРСИ:")
        
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            self.stdout.write(f"  🖥️ CPU: {cpu_percent:.1f}%")
            
            # Пам"ять
            memory = psutil.virtual_memory()
            self.stdout.write(f"  🧠 Пам"ять: {memory.percent:.1f}% ({memory.used//1024//1024:,}MB / {memory.total//1024//1024:,}MB)")
            
            # Диск
            disk = psutil.disk_usage("/")
            self.stdout.write(f"  💾 Диск: {disk.percent:.1f}% ({disk.used//1024//1024//1024:,}GB / {disk.total//1024//1024//1024:,}GB)")
            
            # Python процеси
            python_processes = []
            for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_info"]):
                try:
                    if "python" in proc.info["name"].lower():
                        python_processes.append(proc)
                except:
                    continue
            
            self.stdout.write(f"  🐍 Python процесів: {len(python_processes)}")
            
        except Exception as e:
            self.stdout.write(f"  ❌ Помилка: {e}")
    
    def show_recommendations(self):
        """Показує рекомендації для системи"""
        self.stdout.write("\n💡 РЕКОМЕНДАЦІЇ:")
        
        try:
            # Перевіряємо статистику
            total_decisions = TrackedCourtDecision.objects.count()
            with_resolution = TrackedCourtDecision.objects.exclude(
                resolution_text__isnull=True
            ).exclude(resolution_text="").count()
            
            completion_rate = with_resolution / total_decisions * 100 if total_decisions > 0 else 0
            
            if completion_rate >= 99.5:
                self.stdout.write("  ✅ Система повністю оптимізована")
                self.stdout.write("  ✅ Всі резолютивні частини витягнуто")
                self.stdout.write("  ✅ Можна працювати в економному режимі")
            elif completion_rate >= 95:
                self.stdout.write("  🟡 Система майже оптимізована")
                self.stdout.write("  🟡 Можна перейти на економний режим моніторингу")
            else:
                self.stdout.write("  🔴 Потрібна додаткова обробка")
                self.stdout.write("  🔴 Рекомендується запустити витягування резолютивних частин")
            
            # Перевіряємо процеси
            processes = SystemProcessControl.objects.filter(status="running")
            if processes.count() == 0:
                self.stdout.write("  ✅ Немає активних процесів - низьке навантаження")
            
            # Загальні рекомендації
            self.stdout.write("\n🎯 ПОТОЧНИЙ РЕЖИМ РОБОТИ:")
            self.stdout.write("  • Оптимізований файловий моніторинг (кожні 10 хвилин)")
            self.stdout.write("  • Економна статистика (кожні 30 хвилин)")
            self.stdout.write("  • Мінімальне навантаження на систему")
            self.stdout.write("  • Автоматичне реагування на зміни файлів")
            
        except Exception as e:
            self.stdout.write(f"  ❌ Помилка: {e}")
        
        # Фінальний статус
        self.stdout.write(
            self.style.SUCCESS(
                f"\n🎉 СИСТЕМА ОПТИМІЗОВАНА ТА ГОТОВА ДО РОБОТИ!"
                f"\n   Час перевірки: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
            )
        )