"""
Management command для очищення кешованої статистики судових рішень.
"""

from django.core.management.base import BaseCommand
from bankruptcy.models import CourtDecisionStatistics
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = "Очищає кешовану статистику судових рішень"

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            type=str,
            choices=["general", "yearly", "courts", "categories", "justice_kinds", "recent", "all"],
            default="all",
            help="Тип статистики для очищення (за замовчуванням: all)"
        )
        parser.add_argument(
            "--expired-only",
            action="store_true",
            help="Очистити тільки застарілу статистику"
        )
        parser.add_argument(
            "--older-than",
            type=int,
            help="Очистити статистику старшу за N днів"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показати що буде видалено без фактичного видалення"
        )

    def handle(self, *args, **options):
        stat_type = options["type"]
        expired_only = options["expired_only"]
        older_than = options["older_than"]
        dry_run = options["dry_run"]

        self.stdout.write("🗑️ Очищення кешу статистики судових рішень...")

        # Базовий queryset
        queryset = CourtDecisionStatistics.objects.all()
        
        # Фільтруємо за типом
        if stat_type != "all":
            queryset = queryset.filter(stat_type=stat_type)
        
        # Фільтруємо за застарілістю
        if expired_only:
            now = datetime.now()
            queryset = queryset.filter(
                models.Q(is_valid=False) |
                models.Q(cache_expires_at__lt=now)
            )
        
        # Фільтруємо за віком
        if older_than:
            cutoff_date = datetime.now() - timedelta(days=older_than)
            queryset = queryset.filter(updated_at__lt=cutoff_date)
        
        count = queryset.count()
        
        if count == 0:
            self.stdout.write(self.style.SUCCESS("✅ Нічого не знайдено для очищення"))
            return
        
        if dry_run:
            self.stdout.write(f"🔍 Буде видалено {count} записів:")
            for stat in queryset[:10]:  # Показуємо перші 10
                expires_info = ""
                if stat.cache_expires_at:
                    if stat.cache_expires_at < datetime.now():
                        expires_info = " (застарів)"
                    else:
                        expires_info = f" (діє до {stat.cache_expires_at.strftime("%d.%m %H:%M")})"
                
                self.stdout.write(
                    f"   - {stat.get_stat_type_display()}: "
                    f"{stat.records_count} записів{expires_info}"
                )
            
            if count > 10:
                self.stdout.write(f"   ... та ще {count - 10} записів")
                
            self.stdout.write("\nДля фактичного видалення запустіть без --dry-run")
            return
        
        # Видаляємо записи
        deleted_stats = {}
        for stat in queryset:
            stat_type_name = stat.get_stat_type_display()
            if stat_type_name not in deleted_stats:
                deleted_stats[stat_type_name] = 0
            deleted_stats[stat_type_name] += 1
        
        queryset.delete()
        
        self.stdout.write(self.style.SUCCESS(f"✅ Видалено {count} записів кешу:"))
        for stat_name, count in deleted_stats.items():
            self.stdout.write(f"   - {stat_name}: {count}")
        
        # Показуємо залишки
        remaining = CourtDecisionStatistics.objects.filter(is_valid=True).count()
        self.stdout.write(f"\n📊 Залишилось валідних записів кешу: {remaining}")