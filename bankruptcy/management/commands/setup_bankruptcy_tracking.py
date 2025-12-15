from django.core.management.base import BaseCommand
from django.db import transaction
import logging

from bankruptcy.services import BankruptcyAutoTrackingService
from bankruptcy.models import TrackedBankruptcyCase, BankruptcyCase

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Налаштовує автоматичне відстеження для всіх справ банкрутства"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показує що буде зроблено без фактичних змін",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Перезапускає пошук для всіх справ, навіть якщо вони вже відстежуються",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Обмежує кількість справ для обробки",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        force = options["force"]
        limit = options["limit"]

        if dry_run:
            self.stdout.write(
                self.style.WARNING("*** РЕЖИМ DRY-RUN - ЗМІНИ НЕ БУДУТЬ ЗАСТОСОВАНІ ***")
            )

        self.stdout.write("Початок налаштування відстеження справ банкрутства...")

        try:
            # Отримуємо статистику
            total_bankruptcy_cases = BankruptcyCase.objects.count()
            already_tracked = TrackedBankruptcyCase.objects.count()
            
            self.stdout.write(f"Всього справ банкрутства: {total_bankruptcy_cases}")
            self.stdout.write(f"Вже відстежується: {already_tracked}")
            self.stdout.write(f"Потребують налаштування: {total_bankruptcy_cases - already_tracked}")

            if dry_run:
                # В режимі dry-run тільки показуємо статистику
                if force:
                    self.stdout.write("При використанні --force буде перезапущено пошук для всіх справ")
                
                if limit:
                    self.stdout.write(f"Буде оброблено максимум {limit} справ")
                    
                return

            # Ініціалізуємо сервіс
            service = BankruptcyAutoTrackingService()

            if force:
                # Режим force - скидаємо статуси пошуку для всіх справ
                self.stdout.write("Скидання статусів пошуку для всіх відстежуваних справ...")
                
                with transaction.atomic():
                    updated_count = TrackedBankruptcyCase.objects.filter(
                        search_decisions_status="completed"
                    ).update(
                        search_decisions_status="pending",
                        search_decisions_found=0,
                        search_decisions_completed_at=None
                    )
                    
                    self.stdout.write(
                        self.style.SUCCESS(f"Скинуто статуси для {updated_count} справ")
                    )

            # Налаштовуємо відстеження для всіх справ
            result = service.setup_tracking_for_all_bankruptcy_cases()

            if result["success"]:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Налаштування завершено успішно:\n"
                        f"- Всього справ: {result["total_cases"]}\n"
                        f"- Створено нових відстежень: {result["created_tracking"]}\n"
                        f"- Існуючих відстежень: {result["existing_tracking"]}"
                    )
                )

                if result["created_tracking"] > 0:
                    self.stdout.write(
                        "Фонові задачі пошуку судових рішень запущено для нових справ"
                    )

            else:
                self.stdout.write(
                    self.style.ERROR(f"Помилка налаштування: {result.get("error", "Невідома помилка")}")
                )
                return

            # Показуємо оновлену статистику
            self.stdout.write("\nФінальна статистика:")
            
            from bankruptcy.services import BankruptcyCaseSearchService
            search_service = BankruptcyCaseSearchService()
            stats = search_service.get_statistics()
            
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
            logger.error(f"Помилка команди setup_bankruptcy_tracking: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")