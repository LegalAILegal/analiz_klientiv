import os
import csv
import logging
import tempfile
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection
from django.utils import timezone

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "УЛЬТРАШВИДКИЙ імпорт судових рішень через PostgreSQL COPY"

    def add_arguments(self, parser):
        parser.add_argument(
            "--year", 
            type=int, 
            help="Рік для імпорту (наприклад, 2024)",
            required=True
        )

    def handle(self, *args, **options):
        year = options["year"]
        
        # Визначаємо шлях до CSV файлу
        if year >= 2000:
            short_year = year - 2000
        else:
            short_year = year - 1900
        
        csv_filename = f"documents_{short_year:02d}.csv"
        csv_path = os.path.join(settings.BASE_DIR, "data", csv_filename)
        
        if not os.path.exists(csv_path):
            self.stdout.write(
                self.style.ERROR(f"Файл {csv_filename} не знайдено в директорії data/")
            )
            return
        
        self.stdout.write(f"🚀 УЛЬТРАШВИДКИЙ ІМПОРТ судових рішень за {year} рік")
        self.stdout.write(f"📁 Файл: {csv_filename}")
        
        import time
        start_time = time.time()
        
        # Створення та підготовка таблиці
        table_name = self._create_ultra_fast_table(year)
        
        # Конвертуємо та імпортуємо
        imported_count = self._direct_copy_import(csv_path, table_name, year)
        
        # Створюємо індекси
        self._create_indexes(table_name)
        
        end_time = time.time()
        duration = end_time - start_time
        records_per_second = imported_count / duration if duration > 0 else 0
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🎯 УЛЬТРАШВИДКИЙ ІМПОРТ завершено за {duration:.1f} сек!\n"
                f"📈 Імпортовано: {imported_count:,} записів\n"
                f"⚡ Швидкість: {records_per_second:,.0f} записів/сек"
            )
        )

    def _create_ultra_fast_table(self, year):
        """Створення максимально оптимізованої таблиці"""
        table_name = f"court_decisions_{year}"
        
        with connection.cursor() as cursor:
            # Видаляємо таблицю якщо існує для чистого старту
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            
            # Створюємо UNLOGGED таблицю без будь-яких обмежень
            create_table_sql = f"""
                CREATE UNLOGGED TABLE {table_name} (
                    doc_id VARCHAR(50),
                    court_code VARCHAR(20),
                    judgment_code VARCHAR(10),
                    justice_kind VARCHAR(10),
                    category_code VARCHAR(20),
                    cause_num VARCHAR(255),
                    adjudication_date TIMESTAMP,
                    receipt_date TIMESTAMP,
                    judge VARCHAR(500),
                    doc_url TEXT,
                    status VARCHAR(10),
                    date_publ TIMESTAMP,
                    import_date TIMESTAMP DEFAULT NOW()
                );
            """
            
            cursor.execute(create_table_sql)
            
            # Максимальна оптимізація PostgreSQL для швидкості
            optimizations = [
                "SET maintenance_work_mem = "1GB";",
                "SET work_mem = "256MB";",
                "SET shared_buffers = "256MB";",
                "SET effective_cache_size = "1GB";",
            ]
            
            for opt in optimizations:
                try:
                    cursor.execute(opt)
                except Exception as e:
                    # Ігноруємо помилки налаштувань
                    pass
            
            self.stdout.write(f"🆕 Створено ультрашвидку таблицю {table_name}")
        
        return table_name

    def _direct_copy_import(self, csv_path, table_name, year):
        """Прямий імпорт через PostgreSQL COPY з мінімальною обробкою"""
        
        with connection.cursor() as cursor:
            # Підготовлюємо тимчасовий файл для COPY
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
                temp_path = temp_file.name
                
                self.stdout.write(f"🔄 Конвертація CSV для прямого COPY...")
                
                record_count = 0
                
                with open(csv_path, "r", encoding="utf-8") as source_file:
                    reader = csv.DictReader(source_file, delimiter="\t")
                    
                    for i, row in enumerate(reader):
                        # Мінімальна обробка даних
                        processed_data = [
                            self._clean_value(row.get("doc_id", ""))[:50],
                            self._clean_value(row.get("court_code", ""))[:20],
                            self._clean_value(row.get("judgment_code", ""))[:10],
                            self._clean_value(row.get("justice_kind", ""))[:10],
                            self._clean_value(row.get("category_code", ""))[:20],
                            self._clean_value(row.get("cause_num", ""))[:255],
                            self._parse_date_simple(row.get("adjudication_date", "")),
                            self._parse_date_simple(row.get("receipt_date", "")),
                            self._clean_value(row.get("judge", ""))[:500],
                            self._clean_value(row.get("doc_url", "")),
                            self._clean_value(row.get("status", ""))[:10],
                            self._parse_date_simple(row.get("date_publ", "")),
                            timezone.now().isoformat()
                        ]
                        
                        # Записуємо в тимчасовий файл
                        csv_line = "\t".join(str(val) if val else "" for val in processed_data)
                        temp_file.write(csv_line + "\n")
                        record_count += 1
                        
                        # Показуємо прогрес кожні 100К записів
                        if i % 100000 == 0 and i > 0:
                            self.stdout.write(f"⚡ Оброблено {i:,} записів...")
                
                self.stdout.write(f"📊 Конвертовано {record_count:,} записів")
            
            # Виконуємо COPY напряму з файлу
            self.stdout.write(f"🔥 Прямий COPY в PostgreSQL...")
            
            copy_sql = f"""
                COPY {table_name} 
                (doc_id, court_code, judgment_code, justice_kind, category_code, 
                 cause_num, adjudication_date, receipt_date, judge, doc_url, 
                 status, date_publ, import_date)
                FROM "{temp_path}" 
                WITH (FORMAT CSV, DELIMITER E"\\t", NULL "")
            """
            
            cursor.execute(copy_sql)
            
            # Видаляємо тимчасовий файл
            os.unlink(temp_path)
            
            return record_count

    def _clean_value(self, value):
        """Мінімальне очищення значення"""
        if not value:
            return ""
        return str(value).strip().replace("\n", " ").replace("\r", " ")

    def _parse_date_simple(self, date_str):
        """Спрощений парсинг дати"""
        if not date_str or date_str.strip() == "":
            return None
        
        try:
            # Беремо тільки дату без часової зони
            if "+" in date_str:
                date_part = date_str.split("+")[0].strip(""")
            elif date_str.startswith(""") and date_str.endswith("""):
                date_part = date_str[1:-1]
            else:
                date_part = date_str
            
            # Парсимо стандартний формат
            if len(date_part) >= 19:  # "2024-01-01 00:00:00"
                return date_part[:19]
            elif len(date_part) >= 10:  # "2024-01-01"
                return date_part[:10] + " 00:00:00"
            
            return None
        except:
            return None

    def _create_indexes(self, table_name):
        """Швидке створення індексів після імпорту"""
        self.stdout.write("⚡ Створення індексів...")
        
        with connection.cursor() as cursor:
            # Створюємо тільки найнеобхідніші індекси
            indexes_sql = [
                f"CREATE UNIQUE INDEX CONCURRENTLY idx_{table_name}_doc_id ON {table_name} (doc_id);",
                f"CREATE INDEX CONCURRENTLY idx_{table_name}_cause_num ON {table_name} USING btree (cause_num);",
                f"CREATE INDEX CONCURRENTLY idx_{table_name}_search ON {table_name} USING btree (cause_num, adjudication_date);",
            ]
            
            for index_sql in indexes_sql:
                try:
                    cursor.execute(index_sql)
                    self.stdout.write("✅ Індекс створено")
                except Exception as e:
                    self.stdout.write(f"⚠️ Помилка індексу: {e}")
            
            # Повертаємо таблицю в LOGGED режим
            cursor.execute(f"ALTER TABLE {table_name} SET LOGGED;")
            
            # Оновлюємо статистику для оптимізатора
            cursor.execute(f"ANALYZE {table_name};")
        
        self.stdout.write("✅ Індекси та статистика готові!")