from django.core.management.base import BaseCommand
from django.db import transaction
from bankruptcy.models import TrackedBankruptcyCase, TrackedCourtDecision
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Виправляє неправильні значення search_decisions_found"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Тільки показати проблеми, не виправляти",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write("РЕЖИМ ТЕСТУВАННЯ - зміни не зберігаються")
        else:
            self.stdout.write("РЕЖИМ ВИПРАВЛЕННЯ - зміни будуть збережені")

        # Знаходимо всі відстежувані справи
        all_cases = TrackedBankruptcyCase.objects.all()
        total_cases = all_cases.count()

        self.stdout.write(f"Перевіряємо {total_cases} справ...")

        problems_found = 0
        cases_fixed = 0

        for i, tracked_case in enumerate(all_cases, 1):
            if i % 1000 == 0:
                self.stdout.write(f"Оброблено {i}/{total_cases} справ...")

            # Рахуємо реальну кількість судових рішень
            real_count = TrackedCourtDecision.objects.filter(tracked_case=tracked_case).count()
            recorded_count = tracked_case.search_decisions_found or 0

            if real_count != recorded_count:
                problems_found += 1
                case_number = tracked_case.bankruptcy_case.case_number

                self.stdout.write(
                    f"Справа {case_number}: записано={recorded_count}, реально={real_count}"
                )

                if not dry_run:
                    # Виправляємо значення
                    tracked_case.search_decisions_found = real_count
                    tracked_case.save(update_fields=['search_decisions_found'])
                    cases_fixed += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"\nВиконано:\n"
                f"- Знайдено проблем: {problems_found}\n"
                f"- Виправлено справ: {cases_fixed if not dry_run else 0}\n"
                f"- Режим: {'тестування' if dry_run else 'виправлення'}"
            )
        )