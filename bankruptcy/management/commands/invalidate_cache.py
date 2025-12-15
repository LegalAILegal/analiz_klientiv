"""
Management command для ручної інвалідації кешу статистики
"""

from django.core.management.base import BaseCommand
from bankruptcy.models import CourtDecisionStatistics


class Command(BaseCommand):
    help = "Інвалідує кеш статистики судових рішень (примушує до перерахунку)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            type=str,
            choices=["general", "yearly", "courts", "categories", "justice_kinds", "recent", "all"],
            default="all",
            help="Тип статистики для інвалідації (за замовчуванням: all)"
        )

    def handle(self, *args, **options):
        stat_type = options["type"]
        
        self.stdout.write("🗑️ Інвалідація кешу статистики...")
        
        # Базовий queryset
        queryset = CourtDecisionStatistics.objects.filter(is_valid=True)
        
        # Фільтруємо за типом
        if stat_type != "all":
            queryset = queryset.filter(stat_type=stat_type)
        
        count = queryset.count()
        
        if count == 0:
            self.stdout.write(self.style.SUCCESS("✅ Немає валідного кешу для інвалідації"))
            return
        
        # Інвалідуємо кеш
        queryset.update(is_valid=False)
        
        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Інвалідовано {count} записів кешу типу "{stat_type}"\n"
                f"   При наступному доступі статистика буде перерахована"
            )
        )