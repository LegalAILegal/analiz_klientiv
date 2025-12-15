from django.core.management.base import BaseCommand
from django.utils import timezone
from bankruptcy.utils.fast_court_search import FastCourtSearch, get_search_statistics
from bankruptcy.models import TrackedBankruptcyCase, TrackedCourtDecision
import time


class Command(BaseCommand):
    help = "Швидкий пошук судових рішень (на основі SR_AI алгоритмів)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help="Максимальна кількість справ для обробки за один запуск"
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            help="Безперервний пошук судових рішень"
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Розмір батчу для обробки"
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=10,
            help="Максимальна кількість потоків для обробки"
        )
        parser.add_argument(
            "--stats",
            action="store_true",
            help="Показати статистику пошуку судових рішень"
        )
        parser.add_argument(
            "--case-number",
            type=str,
            help="Пошук рішень для конкретного номера справи"
        )

    def handle(self, *args, **options):
        if options["stats"]:
            self.show_statistics()
            return

        # Створюємо екземпляр швидкого пошуку
        searcher = FastCourtSearch()
        
        # Налаштовуємо параметри
        if options["batch_size"]:
            searcher.batch_size = options["batch_size"]
        if options["max_workers"]:
            searcher.max_workers = options["max_workers"]

        self.stdout.write(
            self.style.SUCCESS(
                f"Запуск швидкого пошуку судових рішень "
                f"(батч: {searcher.batch_size}, потоків: {searcher.max_workers})"
            )
        )

        try:
            if options["case_number"]:
                self.handle_single_case_search(searcher, options["case_number"])
            elif options["continuous"]:
                self.handle_continuous_search(searcher)
            else:
                self.handle_batch_search(searcher, options["limit"])
        finally:
            searcher.close_connections()

    def handle_single_case_search(self, searcher, case_number):
        """
        Обробляє пошук рішень для конкретної справи
        """
        self.stdout.write(f"Пошук рішень для справи: {case_number}")
        
        # Знаходимо справу в базі
        try:
            case = TrackedBankruptcyCase.objects.filter(
                bankruptcy_case__case_number__icontains=case_number
            ).first()
            
            if not case:
                self.stdout.write(
                    self.style.ERROR(f"Справа з номером "{case_number}" не знайдена")
                )
                return
            
            self.stdout.write(f"Знайдена справа: {case.bankruptcy_case.case_number}")
            
            # Виконуємо пошук
            start_time = time.time()
            found_decisions = searcher.search_single_case(case)
            duration = time.time() - start_time
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"Пошук завершено за {duration:.2f} секунд. "
                    f"Знайдено {len(found_decisions)} нових рішень"
                )
            )
            
            if found_decisions:
                self.stdout.write("\nЗнайдені рішення:")
                for decision in found_decisions:
                    self.stdout.write(
                        f"- {decision.cause_num} | {decision.judge} | "
                        f"{decision.date_decision} | {decision.source_info}"
                    )
        
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка при пошуку справи: {e}")
            )

    def handle_batch_search(self, searcher, limit):
        """
        Обробляє пошук рішень в режимі батчу
        """
        start_time = time.time()
        
        # Показуємо початкову статистику
        stats = get_search_statistics()
        self.stdout.write(
            f"Початкова статистика: {stats["cases_with_decisions"]}/{stats["total_cases"]} "
            f"справ мають рішення ({stats["search_percentage"]:.1f}%)"
        )
        self.stdout.write(f"Справ для пошуку: {stats["pending_cases"]}")
        self.stdout.write(f"Загальна кількість рішень: {stats["total_decisions"]}")

        # Виконуємо пошук
        result = searcher.search_cases_batch(limit)

        # Показуємо результати
        end_time = time.time()
        duration = end_time - start_time

        if result["success"]:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nПошук завершено за {duration:.2f} секунд:\n"
                    f"- Оброблено справ: {result["processed"]}\n"
                    f"- Знайдено рішень: {result["found_decisions"]}"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(f"Помилка: {result.get("error", "Невідома помилка")}")
            )

        # Показуємо фінальну статистику
        final_stats = get_search_statistics()
        self.stdout.write(
            f"\nФінальна статистика: {final_stats["cases_with_decisions"]}/{final_stats["total_cases"]} "
            f"справ мають рішення ({final_stats["search_percentage"]:.1f}%)"
        )
        self.stdout.write(f"Залишилось для пошуку: {final_stats["pending_cases"]}")
        self.stdout.write(f"Загальна кількість рішень: {final_stats["total_decisions"]}")

    def handle_continuous_search(self, searcher):
        """
        Обробляє безперервний пошук судових рішень
        """
        self.stdout.write(
            self.style.SUCCESS("Запуск безперервного пошуку судових рішень...")
        )
        
        try:
            cycle_count = 0
            while True:
                cycle_count += 1
                cycle_start_time = time.time()
                
                self.stdout.write(f"\n=== ЦИКЛ {cycle_count} ===")
                
                # Перевіряємо статистику
                stats = get_search_statistics()
                self.stdout.write(
                    f"Статистика: {stats["cases_with_decisions"]}/{stats["total_cases"]} "
                    f"справ мають рішення ({stats["search_percentage"]:.1f}%)"
                )
                
                if stats["pending_cases"] == 0:
                    self.stdout.write("Всі справи оброблено. Очікування нових справ...")
                    time.sleep(60)
                    continue
                
                self.stdout.write(f"Знайдено {stats["pending_cases"]} справ для пошуку")
                
                # Виконуємо пошук
                result = searcher.search_cases_batch()
                
                cycle_duration = time.time() - cycle_start_time
                
                if result["success"] and result["processed"] > 0:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Цикл {cycle_count} завершено за {cycle_duration:.2f}с: "
                            f"оброблено {result["processed"]} справ, "
                            f"знайдено {result["found_decisions"]} рішень"
                        )
                    )
                else:
                    self.stdout.write(f"Цикл {cycle_count}: немає справ для обробки")
                    time.sleep(30)
                
                # Невелика пауза між циклами
                time.sleep(5)
                
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING("\nБезперервний пошук зупинено користувачем")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка в безперервному пошуку: {e}")
            )

    def show_statistics(self):
        """
        Показує детальну статистику пошуку судових рішень
        """
        stats = get_search_statistics()
        
        # Додаткова статистика
        never_searched = TrackedBankruptcyCase.objects.filter(
            search_decisions_completed_at__isnull=True
        ).count()
        
        old_searches = TrackedBankruptcyCase.objects.filter(
            search_decisions_completed_at__lt=timezone.now() - timezone.timedelta(days=7)
        ).count()
        
        cases_without_numbers = TrackedBankruptcyCase.objects.filter(
            bankruptcy_case__case_number__isnull=True
        ).count()
        
        # Статистика по рокам рішень
        from django.db.models import Count
        from django.db import connection
        
        yearly_stats = []
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    EXTRACT(YEAR FROM created_at) as year,
                    COUNT(*) as count
                FROM bankruptcy_trackedcourtdecision 
                WHERE created_at IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM created_at)
                ORDER BY year DESC
                LIMIT 10
            """)
            yearly_stats = cursor.fetchall()

        self.stdout.write(
            self.style.SUCCESS("=== СТАТИСТИКА ПОШУКУ СУДОВИХ РІШЕНЬ ===")
        )
        self.stdout.write(f"Загальна кількість справ: {stats["total_cases"]}")
        self.stdout.write(f"Справ з рішеннями: {stats["cases_with_decisions"]}")
        self.stdout.write(f"Справ без рішень: {stats["pending_cases"]}")
        self.stdout.write(f"Прогрес пошуку: {stats["search_percentage"]:.1f}%")
        self.stdout.write(f"Загальна кількість рішень: {stats["total_decisions"]}")
        self.stdout.write(f"Знайдено за останню годину: {stats["recent_decisions"]}")
        
        self.stdout.write(
            self.style.WARNING("\n=== СТАТИСТИКА ПОШУКУ ===")
        )
        self.stdout.write(f"Справ які ніколи не шукались: {never_searched}")
        self.stdout.write(f"Справ з застарілим пошуком (>7 днів): {old_searches}")
        self.stdout.write(f"Справ без номера: {cases_without_numbers}")
        
        if yearly_stats:
            self.stdout.write(
                self.style.SUCCESS("\n=== СТАТИСТИКА ПО РОКАХ ===")
            )
            for year, count in yearly_stats:
                if year:
                    self.stdout.write(f"{int(year)}: {count} рішень")
        
        # Показуємо останні знайдені рішення
        recent_decisions = TrackedCourtDecision.objects.filter(
            found_at__isnull=False
        ).order_by("-found_at")[:5]
        
        if recent_decisions:
            self.stdout.write(
                self.style.SUCCESS("\n=== ОСТАННІ ЗНАЙДЕНІ РІШЕННЯ ===")
            )
            for decision in recent_decisions:
                self.stdout.write(
                    f"Справа: {decision.tracked_case.bankruptcy_case.case_number}, "
                    f"Рішення: {decision.cause_num}, "
                    f"Знайдено: {decision.found_at.strftime("%d.%m.%Y %H:%M")}"
                )