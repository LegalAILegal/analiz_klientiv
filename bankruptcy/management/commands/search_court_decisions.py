from django.core.management.base import BaseCommand
from django.db import transaction
from django.db import models
import logging
import time

from bankruptcy.services import BankruptcyCaseSearchService
from bankruptcy.models import TrackedBankruptcyCase

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Виконує пошук судових рішень для відстежуваних справ банкрутства"

    def add_arguments(self, parser):
        parser.add_argument(
            "--case-number",
            type=str,
            help="Номер конкретної справи для пошуку",
        )
        parser.add_argument(
            "--priority-only",
            action="store_true",
            help="Обробляти тільки справи з найвищим пріоритетом",
        )
        parser.add_argument(
            "--failed-only",
            action="store_true",
            help="Повторити пошук тільки для справ з помилками",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Максимальна кількість справ для обробки (за замовчуванням: 10)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=25,
            help="Розмір батча для обробки (за замовчуванням: 25 - ПРИСКОРЕНО)",
        )
        parser.add_argument(
            "--delay",
            type=int,
            default=1,
            help="Затримка між справами в секундах (за замовчуванням: 1 - ПРИСКОРЕНО)",
        )

    def handle(self, *args, **options):
        case_number = options["case_number"]
        priority_only = options["priority_only"]
        failed_only = options["failed_only"]
        limit = options["limit"]
        batch_size = options["batch_size"]
        delay = options["delay"]

        self.stdout.write("Початок пошуку судових рішень...")

        try:
            service = BankruptcyCaseSearchService()

            if case_number:
                # Пошук для конкретної справи
                self.stdout.write(f"Пошук для справи: {case_number}")
                
                try:
                    tracked_case = TrackedBankruptcyCase.objects.get(
                        bankruptcy_case__case_number=case_number
                    )
                    
                    found_decisions = service.search_and_save_court_decisions(tracked_case)

                    # Оновлюємо поле search_decisions_found
                    tracked_case.search_decisions_found = found_decisions
                    tracked_case.search_decisions_status = "completed"
                    tracked_case.save(update_fields=['search_decisions_found', 'search_decisions_status'])

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Пошук завершено для справи {case_number}. "
                            f"Знайдено {found_decisions} рішень."
                        )
                    )
                    
                except TrackedBankruptcyCase.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f"Справа {case_number} не відстежується")
                    )
                    return
                    
            else:
                # Масовий пошук
                queryset = TrackedBankruptcyCase.objects.filter(status="active")
                
                if priority_only:
                    # Тільки справи з найвищим пріоритетом
                    max_priority = TrackedBankruptcyCase.objects.aggregate(
                        max_priority=models.Max("priority")
                    )["max_priority"] or 0
                    
                    queryset = queryset.filter(priority=max_priority)
                    self.stdout.write(f"Фільтр: тільки справи з пріоритетом {max_priority}")
                
                if failed_only:
                    queryset = queryset.filter(search_decisions_status="failed")
                    self.stdout.write("Фільтр: тільки справи з помилками")
                else:
                    # Включаємо справи зі статусом pending, failed, та completed з 0 знайдених рішень
                    from django.db import models
                    queryset = queryset.filter(
                        models.Q(search_decisions_status__in=["pending", "failed"]) |
                        models.Q(search_decisions_status="completed", search_decisions_found=0)
                    )
                
                # ВАЖЛИВО: Сортуємо ВІД НОВИХ ДО СТАРИХ справ (найновіші першими)
                cases_to_process = queryset.order_by("-priority", "bankruptcy_case__date", "created_at")[:limit]
                total_cases = cases_to_process.count()
                
                if total_cases == 0:
                    self.stdout.write("Немає справ для обробки")
                    return
                
                self.stdout.write(f"Знайдено {total_cases} справ для обробки")
                
                # Обробляємо справи батчами
                processed = 0
                total_decisions = 0
                
                for i in range(0, total_cases, batch_size):
                    batch = cases_to_process[i:i + batch_size]
                    self.stdout.write(f"\nОбробка батча {i // batch_size + 1}...")
                    
                    for tracked_case in batch:
                        try:
                            case_num = tracked_case.bankruptcy_case.case_number
                            self.stdout.write(f"Обробляється справа {case_num}...")
                            
                            found_decisions = service.search_and_save_court_decisions(tracked_case)
                            total_decisions += found_decisions
                            processed += 1
                            
                            if found_decisions > 0:
                                self.stdout.write(
                                    self.style.SUCCESS(f"  Знайдено {found_decisions} рішень")
                                )
                            else:
                                self.stdout.write("  Рішень не знайдено")
                            
                            # Затримка між справами
                            if delay > 0:
                                time.sleep(delay)
                                
                        except Exception as e:
                            self.stdout.write(
                                self.style.ERROR(f"  Помилка: {e}")
                            )
                            logger.error(f"Помилка обробки справи {tracked_case.bankruptcy_case.case_number}: {e}")
                            continue
                    
                    self.stdout.write(f"Батч завершено. Оброблено {processed}/{total_cases} справ")
                    
                    # Затримка між батчами
                    if i + batch_size < total_cases and delay > 0:
                        self.stdout.write(f"Затримка {delay} секунд...")
                        time.sleep(delay)
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\nПошук завершено:\n"
                        f"- Оброблено справ: {processed}/{total_cases}\n"
                        f"- Всього знайдено рішень: {total_decisions}"
                    )
                )

            # Показуємо фінальну статистику
            self.stdout.write("\n" + "="*50)
            self.stdout.write("СТАТИСТИКА ПІСЛЯ ПОШУКУ:")
            
            stats = service.get_statistics()
            if stats:
                self.stdout.write(f"Всього відстежуваних справ: {stats["total_tracked_cases"]}")
                self.stdout.write(f"Активних справ: {stats["active_tracked_cases"]}")
                self.stdout.write(f"Знайдено судових рішень: {stats["total_court_decisions"]}")
                
                search_stats = stats["search_status"]
                self.stdout.write("Статуси пошуку:")
                self.stdout.write(f"  - Очікують: {search_stats["pending"]}")
                self.stdout.write(f"  - Виконуються: {search_stats["running"]}")
                self.stdout.write(f"  - Завершені: {search_stats["completed"]}")
                self.stdout.write(f"  - З помилками: {search_stats["failed"]}")

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Критична помилка: {e}")
            )
            logger.error(f"Помилка команди search_court_decisions: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")

    def _get_available_tables(self, year=None):
        """Отримання списку доступних таблиць судових рішень"""
        with connection.cursor() as cursor:
            if year:
                # Шукаємо тільки в таблиці конкретного року
                table_pattern = f"court_decisions_{year}"
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                """, [table_pattern])
            else:
                # Шукаємо всі таблиці судових рішень
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name LIKE 'court_decisions_%'
                    ORDER BY table_name DESC
                """)
            
            return [row[0] for row in cursor.fetchall()]

    def _search_in_table(self, table_name, case_number):
        """Пошук в конкретній таблиці"""
        with connection.cursor() as cursor:
            # Використовуємо ILIKE для нечіткого пошуку
            search_sql = f"""
                SELECT doc_id, court_code, judgment_code, cause_num, 
                       adjudication_date, judge, doc_url, status,
                       court_name, judgment_name, resolution_text,
                       "{table_name}" as source_table
                FROM {table_name}
                WHERE cause_num ILIKE %s
                ORDER BY adjudication_date DESC
            """
            
            try:
                cursor.execute(search_sql, [f"%{case_number}%"])
                
                columns = [desc[0] for desc in cursor.description]
                results = []
                
                for row in cursor.fetchall():
                    result_dict = dict(zip(columns, row))
                    results.append(result_dict)
                
                return results
                
            except Exception as e:
                logger.error(f"Помилка пошуку в таблиці {table_name}: {e}")
                return []

    def _display_results(self, results):
        """Виведення результатів пошуку"""
        for i, result in enumerate(results, 1):
            self.stdout.write(f"\n--- Рішення #{i} ---")
            self.stdout.write(f"ID документа: {result["doc_id"]}")
            self.stdout.write(f"Номер справи: {result["cause_num"]}")
            self.stdout.write(f"Дата рішення: {result["adjudication_date"]}")
            self.stdout.write(f"Суддя: {result["judge"]}")
            self.stdout.write(f"Суд: {result["court_name"] or result["court_code"]}")
            self.stdout.write(f"Тип рішення: {result["judgment_name"] or result["judgment_code"]}")
            self.stdout.write(f"Статус: {result["status"]}")
            self.stdout.write(f"URL документа: {result["doc_url"]}")
            self.stdout.write(f"Джерело: {result["source_table"]}")
            
            # ЄДРСР посилання
            if result["doc_id"]:
                yedr_url = f"https://reyestr.court.gov.ua/Review/{result["doc_id"]}"
                self.stdout.write(f"ЄДРСР посилання: {yedr_url}")
            
            # Резолютивна частина (якщо є)
            if result.get("resolution_text") and result["resolution_text"].strip():
                self.stdout.write(f"Резолютивна частина: {result["resolution_text"][:200]}...")

    def _export_to_csv(self, results, csv_path):
        """Експорт результатів у CSV файл"""
        import csv
        
        if not results:
            return
        
        try:
            with open(csv_path, "w", encoding="utf-8", newline="") as csvfile:
                # Визначаємо поля для експорту
                fieldnames = [
                    "doc_id", "cause_num", "adjudication_date", "judge", 
                    "court_code", "court_name", "judgment_code", "judgment_name",
                    "status", "doc_url", "yedr_url", "resolution_text", "source_table"
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in results:
                    # Додаємо ЄДРСР посилання
                    result["yedr_url"] = f"https://reyestr.court.gov.ua/Review/{result["doc_id"]}" if result["doc_id"] else ""
                    
                    # Записуємо тільки потрібні поля
                    export_row = {field: result.get(field, "") for field in fieldnames}
                    writer.writerow(export_row)
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка експорту в CSV: {e}"))