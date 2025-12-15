from django.core.management.base import BaseCommand
from bankruptcy.fast_monitor import (
    start_fast_monitoring, 
    stop_fast_monitoring, 
    get_monitoring_status,
    trigger_court_decisions_processing
)
import time
import json


class Command(BaseCommand):
    help = "Швидкий сервіс моніторингу та автоматичної обробки (на основі SR_AI алгоритмів)"

    def add_arguments(self, parser):
        parser.add_argument(
            "action",
            choices=["start", "stop", "status", "restart", "trigger"],
            help="Дія для виконання"
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Рік для тригера обробки судових рішень"
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            help="Безперервний режим (для start)"
        )

    def handle(self, *args, **options):
        action = options["action"]
        
        if action == "start":
            self.handle_start(options)
        elif action == "stop":
            self.handle_stop()
        elif action == "status":
            self.handle_status()
        elif action == "restart":
            self.handle_restart(options)
        elif action == "trigger":
            self.handle_trigger(options)

    def handle_start(self, options):
        """Запускає швидкий сервіс моніторингу"""
        self.stdout.write(
            self.style.SUCCESS("Запуск швидкого сервісу моніторингу...")
        )
        
        try:
            service = start_fast_monitoring()
            
            self.stdout.write(
                self.style.SUCCESS("Швидкий сервіс моніторингу запущено успішно")
            )
            
            if options["continuous"]:
                self.stdout.write(
                    self.style.WARNING(
                        "Режим безперервного моніторингу. Натисніть Ctrl+C для зупинки."
                    )
                )
                
                try:
                    while service.is_running:
                        time.sleep(10)
                        status = service.get_status()
                        
                        if status["total_search_cycles"] % 10 == 0 and status["total_search_cycles"] > 0:
                            self.stdout.write(
                                f"Статус: пошук циклів {status["total_search_cycles"]}, "
                                f"витягування циклів {status["total_extraction_cycles"]}, "
                                f"знайдено {status["total_found_decisions"]} рішень, "
                                f"витягнуто {status["total_extracted_resolutions"]} резолютивних частин"
                            )
                
                except KeyboardInterrupt:
                    self.stdout.write(
                        self.style.WARNING("\nЗупинка сервісу за запитом користувача...")
                    )
                    stop_fast_monitoring()
                    self.stdout.write(
                        self.style.SUCCESS("Сервіс зупинено")
                    )
        
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка запуску сервісу: {e}")
            )

    def handle_stop(self):
        """Зупиняє швидкий сервіс моніторингу"""
        self.stdout.write("Зупинка швидкого сервісу моніторингу...")
        
        try:
            stop_fast_monitoring()
            self.stdout.write(
                self.style.SUCCESS("Швидкий сервіс моніторингу зупинено")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка зупинки сервісу: {e}")
            )

    def handle_status(self):
        """Показує статус швидкого сервісу моніторингу"""
        try:
            status = get_monitoring_status()
            
            self.stdout.write(
                self.style.SUCCESS("=== СТАТУС ШВИДКОГО СЕРВІСУ МОНІТОРИНГУ ===")
            )
            
            # Основний статус
            status_text = "ПРАЦЮЄ" if status["is_running"] else "ЗУПИНЕНО"
            status_style = self.style.SUCCESS if status["is_running"] else self.style.ERROR
            self.stdout.write(f"Статус сервісу: {status_style(status_text)}")
            
            if status["is_running"]:
                # Статистика циклів
                self.stdout.write(f"Циклів пошуку виконано: {status["total_search_cycles"]}")
                self.stdout.write(f"Циклів витягування виконано: {status["total_extraction_cycles"]}")
                self.stdout.write(f"Останній цикл пошуку: {status["last_search_cycle"] or "Не виконувався"}")
                self.stdout.write(f"Останній цикл витягування: {status["last_extraction_cycle"] or "Не виконувався"}")
                
                # Результати роботи
                self.stdout.write(f"Знайдено рішень: {status["total_found_decisions"]}")
                self.stdout.write(f"Витягнуто резолютивних частин: {status["total_extracted_resolutions"]}")
                
                # Статус потоків
                search_status = "АКТИВНИЙ" if status["threads"]["search_alive"] else "НЕАКТИВНИЙ"
                extraction_status = "АКТИВНИЙ" if status["threads"]["extraction_alive"] else "НЕАКТИВНИЙ"
                self.stdout.write(f"Потік пошуку: {search_status}")
                self.stdout.write(f"Потік витягування: {extraction_status}")
            
            # Загальна статистика
            self.stdout.write(
                self.style.WARNING("\n=== ЗАГАЛЬНА СТАТИСТИКА ===")
            )
            
            search_stats = status["search_stats"]
            self.stdout.write(f"Справ з рішеннями: {search_stats["cases_with_decisions"]}/{search_stats["total_cases"]} ({search_stats["search_percentage"]:.1f}%)")
            self.stdout.write(f"Справ для пошуку: {search_stats["pending_cases"]}")
            self.stdout.write(f"Загальна кількість рішень: {search_stats["total_decisions"]}")
            self.stdout.write(f"Рішень за останню годину: {search_stats["recent_decisions"]}")
            
            extraction_stats = status["extraction_stats"]
            self.stdout.write(f"Рішень з резолютивними частинами: {extraction_stats["extracted_decisions"]}/{extraction_stats["total_decisions"]} ({extraction_stats["extraction_percentage"]:.1f}%)")
            self.stdout.write(f"Рішень для витягування: {extraction_stats["pending_decisions"]}")
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка отримання статусу: {e}")
            )

    def handle_restart(self, options):
        """Перезапускає швидкий сервіс моніторингу"""
        self.stdout.write("Перезапуск швидкого сервісу моніторингу...")
        
        try:
            # Спочатку зупиняємо
            stop_fast_monitoring()
            time.sleep(2)
            
            # Потім запускаємо
            start_fast_monitoring()
            
            self.stdout.write(
                self.style.SUCCESS("Швидкий сервіс моніторингу перезапущено")
            )
            
            if options["continuous"]:
                self.handle_start(options)
        
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка перезапуску сервісу: {e}")
            )

    def handle_trigger(self, options):
        """Тригерить обробку нових судових рішень"""
        year = options.get("year")
        
        self.stdout.write(
            f"Тригер обробки нових судових рішень" + 
            (f" за {year} рік" if year else "")
        )
        
        try:
            trigger_court_decisions_processing(year)
            
            self.stdout.write(
                self.style.SUCCESS("Обробка нових судових рішень завершена")
            )
        
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка тригера обробки: {e}")
            )