"""
Команда для видалення дублікатів TrackedBankruptcyCase.
Залишає найновіший запис для кожної унікальної справи банкрутства.
"""

from django.core.management.base import BaseCommand
from django.db.models import Count, Max
from bankruptcy.models import TrackedBankruptcyCase
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Видаляє дублікати TrackedBankruptcyCase, залишаючи найновіші записи'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показати що буде видалено без фактичного видалення',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Обмежити кількість оброблених дублікатів',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        limit = options['limit']

        self.stdout.write("🔍 Пошук дублікатів TrackedBankruptcyCase...")

        # Знаходимо всі справи з дублікатами (групуємо по номеру справи)
        duplicates_query = TrackedBankruptcyCase.objects.values('bankruptcy_case__case_number').annotate(
            count=Count('id')
        ).filter(count__gt=1).order_by('-count')

        if limit:
            duplicates_query = duplicates_query[:limit]

        duplicates = list(duplicates_query)

        if not duplicates:
            self.stdout.write(self.style.SUCCESS("✅ Дублікатів не знайдено!"))
            return

        self.stdout.write(f"📊 Знайдено {len(duplicates)} справ з дублікатами")

        total_to_delete = 0
        total_kept = 0

        # Більш ефективний підхід - батчами
        batch_size = 100
        processed = 0

        for dup in duplicates:
            case_number = dup['bankruptcy_case__case_number']
            count = dup['count']

            # Знаходимо всі записи для цієї справи
            all_records = TrackedBankruptcyCase.objects.filter(
                bankruptcy_case__case_number=case_number
            ).order_by('-created_at', '-id')

            if all_records.count() > 1:
                # Залишаємо найновіший запис, видаляємо решту
                latest_record = all_records.first()
                to_delete_ids = list(all_records.exclude(id=latest_record.id).values_list('id', flat=True))
                delete_count = len(to_delete_ids)

                if delete_count > 0:
                    self.stdout.write(f"📋 Справа {case_number}: "
                                    f"{count} записів → залишаю 1 (ID {latest_record.id}), видаляю {delete_count}")

                    if not dry_run:
                        # Видаляємо дублікати батчами (швидше ніж по одному)
                        TrackedBankruptcyCase.objects.filter(id__in=to_delete_ids).delete()
                        logger.info(f"Видалено {delete_count} дублікатів для справи {case_number}")

                    total_to_delete += delete_count
                    total_kept += 1

            processed += 1
            if processed % batch_size == 0:
                self.stdout.write(f"📊 Оброблено {processed}/{len(duplicates)} справ...")
                if not dry_run:
                    # Дозволяємо базі обробити зміни
                    import time
                    time.sleep(0.1)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"🔥 DRY RUN: Буде видалено {total_to_delete} дублікатів, "
                f"залишено {total_kept} унікальних записів"
            ))
            self.stdout.write("Для фактичного видалення запустіть без --dry-run")
        else:
            self.stdout.write(self.style.SUCCESS(
                f"✅ Успішно видалено {total_to_delete} дублікатів! "
                f"Залишено {total_kept} унікальних записів."
            ))

            # Перевіряємо результат
            remaining_duplicates = TrackedBankruptcyCase.objects.values('bankruptcy_case__case_number').annotate(
                count=Count('id')
            ).filter(count__gt=1).count()

            if remaining_duplicates == 0:
                self.stdout.write(self.style.SUCCESS("🎉 ВСІ дублікати видалено!"))
            else:
                self.stdout.write(self.style.WARNING(
                    f"⚠️ Залишилося {remaining_duplicates} справ з дублікатами"
                ))