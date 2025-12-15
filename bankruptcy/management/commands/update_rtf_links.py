from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.db import models
import logging
import time

from bankruptcy.models import TrackedCourtDecision

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Оновлює RTF посилання для судових рішень без посилань"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Розмір батча для обробки (за замовчуванням: 1000)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Максимальна кількість рішень для обробки",
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Обновити RTF тільки для конкретного року",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показати статистику без внесення змін",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        limit = options["limit"]
        year = options["year"]
        dry_run = options["dry_run"]

        self.stdout.write("🔗 Початок оновлення RTF посилань...")

        try:
            # Статистика на початок
            total_without_rtf = TrackedCourtDecision.objects.filter(
                models.Q(doc_url__isnull=True) | models.Q(doc_url="")
            ).count()
            
            self.stdout.write(f"📊 Знайдено {total_without_rtf:,} рішень без RTF посилань")
            
            if total_without_rtf == 0:
                self.stdout.write("✅ Всі рішення мають RTF посилання")
                return
            
            if dry_run:
                self.stdout.write("🔍 Режим перегляду - зміни не будуть внесені")
                self._show_statistics(year)
                return
            
            # Оновлюємо RTF посилання
            updated_count = self._update_rtf_links(batch_size, limit, year)
            
            self.stdout.write(
                self.style.SUCCESS(f"✅ Оновлено RTF посилання для {updated_count:,} рішень")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Критична помилка: {e}")
            )
            logger.error(f"Помилка команди update_rtf_links: {e}")

    def _show_statistics(self, year=None):
        """Показує статистику RTF посилань"""
        with connection.cursor() as cursor:
            # Отримуємо доступні таблиці
            tables = self._get_available_tables(year)
            
            self.stdout.write(f"\n📋 Статистика по таблицях ({len(tables)} таблиць):")
            
            for table_name in tables:
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN doc_url IS NOT NULL AND doc_url != '' THEN 1 END) as with_rtf,
                        COUNT(CASE WHEN doc_url IS NULL OR doc_url = '' THEN 1 END) as without_rtf
                    FROM {table_name}
                """)
                
                result = cursor.fetchone()
                total, with_rtf, without_rtf = result
                
                if without_rtf > 0:
                    self.stdout.write(f"  {table_name}: {without_rtf:,} без RTF з {total:,} рішень")

    def _update_rtf_links(self, batch_size, limit, year):
        """Оновлює RTF посилання в батчах"""
        updated_total = 0
        tables = self._get_available_tables(year)
        
        self.stdout.write(f"🔄 Перевіряю {len(tables)} таблиць судових рішень...")
        
        for table_name in tables:
            updated_in_table = self._update_table_rtf_links(table_name, batch_size, limit)
            updated_total += updated_in_table
            
            if updated_in_table > 0:
                self.stdout.write(f"  ✅ {table_name}: оновлено {updated_in_table:,} RTF посилань")
            
            # Перевіряємо ліміт
            if limit and updated_total >= limit:
                break
        
        return updated_total

    def _update_table_rtf_links(self, table_name, batch_size, limit):
        """Оновлює RTF посилання в конкретній таблиці"""
        updated_count = 0
        
        with connection.cursor() as cursor:
            # Знаходимо рішення без RTF, які є в TrackedCourtDecision
            cursor.execute(f"""
                SELECT DISTINCT source.doc_id, source.doc_url
                FROM {table_name} source
                INNER JOIN bankruptcy_trackedcourtdecision tracked 
                    ON source.doc_id = tracked.doc_id
                WHERE source.doc_url IS NOT NULL 
                    AND source.doc_url != ''
                    AND (tracked.doc_url IS NULL OR tracked.doc_url = '')
                ORDER BY source.doc_id
                {"LIMIT " + str(limit) if limit else ""}
            """)
            
            decisions_to_update = cursor.fetchall()
            
            if not decisions_to_update:
                return 0
            
            # Оновлюємо батчами
            for i in range(0, len(decisions_to_update), batch_size):
                batch = decisions_to_update[i:i + batch_size]
                
                # Підготовляємо SQL для batch update
                update_values = []
                doc_ids = []
                
                for doc_id, doc_url in batch:
                    doc_ids.append(doc_id)
                    update_values.append(f"('{doc_id}', '{doc_url}')")
                
                if update_values:
                    # Виконуємо batch update
                    cursor.execute(f"""
                        UPDATE bankruptcy_trackedcourtdecision 
                        SET doc_url = updates.new_url
                        FROM (VALUES {",".join(update_values)}) AS updates(doc_id, new_url)
                        WHERE bankruptcy_trackedcourtdecision.doc_id = updates.doc_id
                    """)
                    
                    updated_count += cursor.rowcount
                
                # Невелика пауза між батчами
                time.sleep(0.1)
        
        return updated_count

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