import time
import signal
import sys
from django.core.management.base import BaseCommand
from bankruptcy.file_monitor import monitor_service
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Запускає моніторинг CSV файлу для автоматичного оновлення даних"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.should_stop = False
    
    def add_arguments(self, parser):
        parser.add_argument(
            "--daemon",
            action="store_true",
            help="Запускати у фоновому режимі"
        )
    
    def handle_signal(self, signum, frame):
        """Обробка сигналів для коректної зупинки"""
        self.stdout.write(self.style.WARNING("Отримано сигнал зупинки..."))
        self.should_stop = True
        monitor_service.stop_monitoring()
        sys.exit(0)
    
    def handle(self, *args, **options):
        daemon_mode = options["daemon"]
        
        # Налаштування обробки сигналів
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.stdout.write(
            self.style.SUCCESS("Запускаю сервіс моніторингу файлів...")
        )
        
        try:
            # Запускаємо моніторинг
            monitor_service.start_monitoring()
            
            if daemon_mode:
                self.stdout.write(
                    self.style.SUCCESS("Моніторинг запущено в фоновому режимі")
                )
                # У daemon режимі команда завершується, але observer продовжує працювати
                return
            
            self.stdout.write(
                self.style.SUCCESS(
                    "Моніторинг активний. Натисніть Ctrl+C для зупинки."
                )
            )
            
            # Основний цикл моніторингу
            while not self.should_stop:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("Зупинка моніторингу..."))
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка моніторингу: {str(e)}")
            )
        finally:
            monitor_service.stop_monitoring()
            self.stdout.write(
                self.style.SUCCESS("Моніторинг файлів зупинено")
            )