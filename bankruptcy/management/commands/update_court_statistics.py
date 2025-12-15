"""
Management command для оновлення кешованої статистики судових рішень.
Можна запускати по cron для автоматичного оновлення.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from bankruptcy.models import CourtDecisionStatistics
from bankruptcy.views import (
    get_court_decision_tables,
    get_court_decisions_total_stats,
    get_court_decisions_yearly_stats,
    get_top_courts_from_decisions,
    get_court_categories_stats,
    get_justice_kinds_stats,
    get_recent_court_decisions,
    json_serialize_dates,
)
from datetime import datetime, timedelta
from django.utils import timezone
import time
import json


class Command(BaseCommand):
    help = "Оновлює кешовану статистику судових рішень для швидкодії"

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            type=str,
            choices=["general", "yearly", "courts", "categories", "justice_kinds", "recent", "all"],
            default="all",
            help="Тип статистики для оновлення (за замовчуванням: all)"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Примусово оновити навіть валідну статистику"
        )
        parser.add_argument(
            "--cache-hours",
            type=int,
            default=None,
            help="Встановити час життя кешу в годинах"
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Детальний вивід процесу"
        )

    def handle(self, *args, **options):
        start_time = time.time()
        stat_type = options["type"]
        force = options["force"]
        cache_hours = options["cache_hours"]
        verbose = options["verbose"]

        self.stdout.write(f"🔄 Починаємо оновлення статистики судових рішень...")
        
        if verbose:
            self.stdout.write(f"Параметри: type={stat_type}, force={force}, cache_hours={cache_hours}")

        try:
            # Отримуємо список таблиць
            tables = get_court_decision_tables()
            if not tables:
                self.stdout.write(
                    self.style.WARNING("⚠️ Не знайдено таблиць з судовими рішеннями")
                )
                return

            if verbose:
                self.stdout.write(f"Знайдено {len(tables)} таблиць: {", ".join(tables)}")

            # Визначаємо які типи статистики оновлювати
            if stat_type == "all":
                types_to_update = ["general", "yearly", "courts", "categories", "justice_kinds", "recent"]
            else:
                types_to_update = [stat_type]

            updated_count = 0
            skipped_count = 0

            for current_type in types_to_update:
                if verbose:
                    self.stdout.write(f"\n📊 Обробляємо статистику типу "{current_type}"...")

                # Перевіряємо чи потрібно оновлювати
                if not force:
                    cached_stat = CourtDecisionStatistics.get_cached_stat(current_type)
                    if cached_stat:
                        if verbose:
                            self.stdout.write(f"✅ Статистика "{current_type}" вже в кеші і актуальна")
                        skipped_count += 1
                        continue

                # Розраховуємо статистику
                calc_start_time = time.time()
                
                try:
                    data = self._calculate_statistics(current_type, tables, verbose)
                    calc_time = timedelta(seconds=time.time() - calc_start_time)
                    
                    # Визначаємо час життя кешу
                    default_cache_hours = {
                        "general": 6,
                        "yearly": 12,
                        "courts": 24,
                        "categories": 24,
                        "justice_kinds": 24,
                        "recent": 1,
                    }
                    
                    final_cache_hours = cache_hours or default_cache_hours.get(current_type, 6)
                    
                    # Підраховуємо кількість записів
                    records_count = 0
                    if isinstance(data, dict) and "total_decisions" in data:
                        records_count = data["total_decisions"]
                    elif isinstance(data, list):
                        records_count = len(data)

                    # Зберігаємо в кеш
                    CourtDecisionStatistics.set_cached_stat(
                        current_type,
                        json_serialize_dates(data),
                        records_count=records_count,
                        calculation_time=calc_time,
                        cache_hours=final_cache_hours
                    )
                    
                    updated_count += 1
                    
                    if verbose:
                        self.stdout.write(
                            f"✅ Оновлено "{current_type}": "
                            f"{records_count} записів, "
                            f"час розрахунку: {calc_time.total_seconds():.2f}с, "
                            f"кеш на {final_cache_hours}г"
                        )
                    
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Помилка при розрахунку статистики "{current_type}": {e}")
                    )
                    continue

            total_time = time.time() - start_time
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n🎉 Оновлення завершено за {total_time:.2f}с!\n"
                    f"   Оновлено: {updated_count}\n"
                    f"   Пропущено: {skipped_count}\n"
                    f"   Всього типів: {len(types_to_update)}"
                )
            )

            # Показуємо статус кешу
            if verbose:
                self.stdout.write("\n📈 Поточний стан кешу:")
                stats = CourtDecisionStatistics.objects.filter(is_valid=True).order_by("stat_type")
                for stat in stats:
                    expires_info = ""
                    if stat.cache_expires_at:
                        expires_in = stat.cache_expires_at - timezone.now()
                        if expires_in.total_seconds() > 0:
                            expires_info = f" (діє ще {expires_in.total_seconds()/3600:.1f}г)"
                        else:
                            expires_info = " (застарів)"
                    
                    calc_info = ""
                    if stat.calculation_time:
                        calc_info = f" за {stat.calculation_time.total_seconds():.2f}с"
                    
                    self.stdout.write(
                        f"   {stat.get_stat_type_display()}: "
                        f"{stat.records_count} записів{calc_info}{expires_info}"
                    )

        except Exception as e:
            raise CommandError(f"Критична помилка при оновленні статистики: {e}")

    def _calculate_statistics(self, stat_type, tables, verbose=False):
        """Розраховує статистику заданого типу"""
        
        if stat_type == "general":
            if verbose:
                self.stdout.write("   Розраховуємо загальну статистику...")
            return get_court_decisions_total_stats(tables)
        
        elif stat_type == "yearly":
            if verbose:
                self.stdout.write("   Розраховуємо статистику по роках...")
            return get_court_decisions_yearly_stats(tables)
        
        elif stat_type == "courts":
            if verbose:
                self.stdout.write("   Розраховуємо топ судів...")
            return get_top_courts_from_decisions(tables, limit=50)  # Більше для кешу
        
        elif stat_type == "categories":
            if verbose:
                self.stdout.write("   Розраховуємо статистику по категоріях...")
            return get_court_categories_stats(tables)
        
        elif stat_type == "justice_kinds":
            if verbose:
                self.stdout.write("   Розраховуємо статистику по видах судочинства...")
            return get_justice_kinds_stats(tables)
        
        elif stat_type == "recent":
            if verbose:
                self.stdout.write("   Отримуємо останні рішення...")
            return get_recent_court_decisions(tables, limit=20)
        
        else:
            raise ValueError(f"Невідомий тип статистики: {stat_type}")