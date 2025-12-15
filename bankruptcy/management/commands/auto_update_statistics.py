"""
Management command для автоматичного моніторингу змін в базах даних
та оновлення кешованої статистики судових рішень
"""

from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone
from bankruptcy.models import CourtDecisionStatistics
from datetime import datetime, timedelta
import time
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Автоматично моніторить зміни в базах даних і оновлює кеш статистики"

    def add_arguments(self, parser):
        parser.add_argument(
            "--check-interval",
            type=int,
            default=300,  # 5 хвилин
            help="Інтервал перевірки змін (в секундах)"
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Виконати перевірку один раз і вийти"
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Детальний вивід процесу"
        )
        parser.add_argument(
            "--min-changes",
            type=int,
            default=100,
            help="Мінімальна кількість змін для тригера оновлення"
        )

    def handle(self, *args, **options):
        check_interval = options["check_interval"]
        run_once = options["run_once"]
        verbose = options["verbose"]
        min_changes = options["min_changes"]

        self.stdout.write(f"🔍 Початок автоматичного моніторингу змін в базах даних")
        self.stdout.write(f"   Інтервал перевірки: {check_interval} секунд")
        self.stdout.write(f"   Мінімум змін для оновлення: {min_changes}")

        # Запам"ятовуємо час останньої перевірки
        last_check_time = timezone.now()
        
        while True:
            try:
                current_time = timezone.now()
                
                # Перевіряємо зміни з останньої перевірки
                changes_count = self.check_database_changes(last_check_time, current_time, verbose)
                
                if changes_count >= min_changes:
                    self.stdout.write(
                        self.style.WARNING(
                            f"🔄 Виявлено {changes_count} змін з {last_check_time.strftime("%H:%M:%S")}. "
                            f"Інвалідуємо кеш..."
                        )
                    )
                    
                    # Інвалідуємо кеш
                    invalidated = self.invalidate_statistics_cache()
                    
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Інвалідовано {invalidated} записів кешу. "
                            f"При наступному доступі статистика буде перерахована."
                        )
                    )
                    
                elif verbose and changes_count > 0:
                    self.stdout.write(
                        f"📊 Виявлено {changes_count} змін (менше порогу {min_changes})"
                    )
                elif verbose:
                    self.stdout.write(f"✅ Змін не виявлено з {last_check_time.strftime("%H:%M:%S")}")

                last_check_time = current_time
                
                if run_once:
                    break

                # Чекаємо до наступної перевірки
                if verbose:
                    next_check = current_time + timedelta(seconds=check_interval)
                    self.stdout.write(f"💤 Наступна перевірка о {next_check.strftime("%H:%M:%S")}")
                
                time.sleep(check_interval)

            except KeyboardInterrupt:
                self.stdout.write("\n👋 Зупинка моніторингу за запитом користувача")
                break
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Помилка моніторингу: {e}")
                )
                logger.error(f"Помилка автоматичного моніторингу: {e}")
                
                if run_once:
                    break
                    
                time.sleep(check_interval)

    def check_database_changes(self, start_time, end_time, verbose=False):
        """Перевіряє кількість змін в базі даних за період"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_changes,
                        COUNT(CASE WHEN operation_type = 'INSERT' THEN 1 END) as inserts,
                        COUNT(CASE WHEN operation_type = 'UPDATE' THEN 1 END) as updates,
                        COUNT(CASE WHEN operation_type = 'DELETE' THEN 1 END) as deletes,
                        COUNT(DISTINCT table_name) as affected_tables
                    FROM bankruptcy_database_changes
                    WHERE change_timestamp >= %s
                    AND change_timestamp <= %s
                """, [start_time, end_time])
                
                result = cursor.fetchone()
                total_changes, inserts, updates, deletes, affected_tables = result
                
                if verbose and total_changes > 0:
                    self.stdout.write(
                        f"   📈 Детали змін: "
                        f"INSERT={inserts}, UPDATE={updates}, DELETE={deletes}, "
                        f"таблиць={affected_tables}"
                    )
                
                return total_changes
                
        except Exception as e:
            logger.error(f"Помилка при перевірці змін в БД: {e}")
            return 0

    def invalidate_statistics_cache(self):
        """Інвалідує кеш статистики"""
        try:
            # Помічаємо всю статистику як застарілу
            invalidated_count = CourtDecisionStatistics.objects.filter(
                is_valid=True
            ).update(is_valid=False)
            
            logger.info(f"Інвалідовано {invalidated_count} записів кешу через зміни в даних")
            return invalidated_count
            
        except Exception as e:
            logger.error(f"Помилка при інвалідації кешу: {e}")
            return 0

    def get_cache_status(self):
        """Показує поточний стан кешу"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        stat_type,
                        COUNT(*) as count,
                        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_count,
                        MAX(updated_at) as last_updated
                    FROM bankruptcy_courtdecisionstatistics
                    GROUP BY stat_type
                    ORDER BY stat_type
                """)
                
                results = cursor.fetchall()
                
                if results:
                    self.stdout.write("\n📊 Поточний стан кешу:")
                    for stat_type, count, valid_count, last_updated in results:
                        status = "✅" if valid_count > 0 else "❌"
                        last_updated_str = last_updated.strftime("%d.%m %H:%M") if last_updated else "—"
                        self.stdout.write(f"   {status} {stat_type}: {valid_count}/{count} валідні (оновлено: {last_updated_str})")
                
        except Exception as e:
            logger.error(f"Помилка при отриманні статусу кешу: {e}")