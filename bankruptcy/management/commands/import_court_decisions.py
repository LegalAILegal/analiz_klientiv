import os
import csv
import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from bankruptcy.utils.index_optimizer import index_optimizer

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Імпорт судових рішень з CSV файлів"

    def add_arguments(self, parser):
        parser.add_argument(
            "--year", 
            type=int, 
            help="Рік для імпорту (наприклад, 2024)",
            required=True
        )
        parser.add_argument(
            "--force", 
            action="store_true",
            help="Перезаписати існуючі записи"
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10000,
            help="Розмір батчу для bulk insert"
        )
        parser.add_argument(
            "--fast",
            action="store_true",
            help="Ультрашвидкий імпорт через PostgreSQL COPY"
        )

    def handle(self, *args, **options):
        year = options["year"]
        force = options["force"]
        batch_size = options["batch_size"]
        fast = options.get("fast", False)

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

        mode_str = "УЛЬТРАШВИДКИЙ (COPY)" if fast else "ШВИДКИЙ (executemany)"
        self.stdout.write(f"Імпорт судових рішень за {year} рік з файлу {csv_filename}")
        self.stdout.write(f"Режим: {mode_str}")
        self.stdout.write(f"Шлях до файлу: {csv_path}")

        # Підрахунок загальної кількості записів
        total_records = self._count_csv_records(csv_path)
        self.stdout.write(f"Всього записів у CSV: {total_records}")

        # Створення індексу для швидкого пошуку в базі даних PostgreSQL
        self._create_temp_table_if_needed(year)

        # Імпорт даних
        if fast:
            imported_count = self._fast_import_via_copy(csv_path, year, total_records)
        else:
            imported_count = self._import_csv_data(
                csv_path, year, force, batch_size, total_records
            )

        self.stdout.write(
            self.style.SUCCESS(f"Імпорт завершено. Оброблено {imported_count} записів.")
        )

        # Автоматична оптимізація індексів після великого імпорту
        if imported_count > 0:
            self.stdout.write("Перевірка необхідності оптимізації індексів...")
            optimization_result = index_optimizer.optimize_after_import(year, imported_count)

            if optimization_result:
                self.stdout.write(
                    self.style.SUCCESS("Автоматична оптимізація індексів завершена!")
                )
            else:
                self.stdout.write("Оптимізація індексів пропущена (недостатньо змін або занадто рано)")  

    def _count_csv_records(self, csv_path):
        """Підрахунок кількості записів у CSV файлі"""
        try:
            with open(csv_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file, delimiter="\t")
                return sum(1 for _ in reader)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка читання CSV: {e}"))
            return 0

    def _create_temp_table_if_needed(self, year):
        """Створення таблиці для судових рішень якщо вона не існує"""
        from django.db import connection
        
        table_name = f"court_decisions_{year}"
        
        with connection.cursor() as cursor:
            # Перевіряємо чи існує таблиця
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """, [table_name])
            
            if not cursor.fetchone()[0]:
                # Створюємо таблицю
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        doc_id VARCHAR(50) UNIQUE NOT NULL,
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
                        court_name VARCHAR(500),
                        judgment_name VARCHAR(200),
                        justice_kind_name VARCHAR(200),
                        category_name VARCHAR(200),
                        resolution_text TEXT,
                        import_date TIMESTAMP DEFAULT NOW()
                    );
                """
                
                try:
                    cursor.execute(create_table_sql)
                except Exception as e:
                    logger.warning(f"Помилка при створенні таблиці {table_name}: {e}")
                    # Перевіряємо чи таблиця все ж існує
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        );
                    """, [table_name])
                    if not cursor.fetchone()[0]:
                        raise  # Якщо таблиця не створилася, кидаємо помилку
                
                # Створюємо індекси для оптимізації
                indexes_sql = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_doc_id ON {table_name} (doc_id);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_cause_num ON {table_name} (cause_num);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_court_code ON {table_name} (court_code);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_adjudication_date ON {table_name} (adjudication_date);",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_case_search ON {table_name} (cause_num, court_code, adjudication_date);"
                ]
                
                for index_sql in indexes_sql:
                    try:
                        cursor.execute(index_sql)
                    except Exception as e:
                        logger.warning(f"Не вдалося створити індекс: {e}")
                
                self.stdout.write(f"Створено таблицю {table_name} з індексами")

    def _import_csv_data(self, csv_path, year, force, batch_size, total_records):
        """Імпорт даних з CSV файлу"""
        from django.db import connection
        
        table_name = f"court_decisions_{year}"
        imported_count = 0
        batch_data = []
        
        try:
            with open(csv_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file, delimiter="\t")
                
                for i, row in enumerate(reader, 1):
                    # Обробляємо рядок даних
                    processed_row = self._process_csv_row(row)
                    batch_data.append(processed_row)
                    
                    # Коли набираємо батч - вставляємо в базу
                    if len(batch_data) >= batch_size:
                        inserted = self._insert_batch(table_name, batch_data, force)
                        imported_count += inserted
                        batch_data = []
                        
                        # Показуємо прогрес
                        if i % (batch_size * 10) == 0:
                            progress = (i / total_records) * 100
                            self.stdout.write(f"Прогрес: {progress:.1f}% ({i}/{total_records})")
                
                # Вставляємо останній батч
                if batch_data:
                    inserted = self._insert_batch(table_name, batch_data, force)
                    imported_count += inserted
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка імпорту: {e}"))
            return imported_count
        
        return imported_count

    def _process_csv_row(self, row):
        """Обробка одного рядка CSV"""
        def parse_date(date_str):
            """Парсинг дати з різних форматів"""
            if not date_str or date_str.strip() == "":
                return None
            
            try:
                # Формат: "2024-01-01 00:00:00+02"
                if "+" in date_str or "-" in date_str[-6:]:
                    # Забираємо часову зону
                    date_part = date_str.split("+")[0].split(" ")[0] if "+" in date_str else date_str.split("-")[0]
                    return datetime.strptime(date_part, "%Y-%m-%d")
                # Простий формат
                elif len(date_str) >= 10:
                    return datetime.strptime(date_str[:10], "%Y-%m-%d")
                
                return None
            except:
                return None

        return {
            "doc_id": row.get("doc_id", "").strip()[:50],
            "court_code": row.get("court_code", "").strip()[:20],
            "judgment_code": row.get("judgment_code", "").strip()[:10],
            "justice_kind": row.get("justice_kind", "").strip()[:10],
            "category_code": row.get("category_code", "").strip()[:20],
            "cause_num": row.get("cause_num", "").strip()[:255],
            "adjudication_date": parse_date(row.get("adjudication_date", "")),
            "receipt_date": parse_date(row.get("receipt_date", "")),
            "judge": row.get("judge", "").strip()[:500],
            "doc_url": row.get("doc_url", "").strip(),
            "status": row.get("status", "").strip()[:10],
            "date_publ": parse_date(row.get("date_publ", "")),
            "court_name": "",  # Заповнюватиметься з довідників
            "judgment_name": "",
            "justice_kind_name": "",
            "category_name": "",
            "resolution_text": "",
            "import_date": timezone.now()
        }

    def _insert_batch(self, table_name, batch_data, force):
        """Вставка батчу даних в таблицю (оптимізовано через executemany)"""
        from django.db import connection

        if not batch_data:
            return 0

        # Підготовляємо SQL запит
        if force:
            insert_sql = f"""
                INSERT INTO {table_name}
                (doc_id, court_code, judgment_code, justice_kind, category_code,
                 cause_num, adjudication_date, receipt_date, judge, doc_url,
                 status, date_publ, court_name, judgment_name, justice_kind_name,
                 category_name, resolution_text, import_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    court_code = EXCLUDED.court_code,
                    judgment_code = EXCLUDED.judgment_code,
                    justice_kind = EXCLUDED.justice_kind,
                    category_code = EXCLUDED.category_code,
                    cause_num = EXCLUDED.cause_num,
                    adjudication_date = EXCLUDED.adjudication_date,
                    receipt_date = EXCLUDED.receipt_date,
                    judge = EXCLUDED.judge,
                    doc_url = EXCLUDED.doc_url,
                    status = EXCLUDED.status,
                    date_publ = EXCLUDED.date_publ,
                    import_date = EXCLUDED.import_date
            """
        else:
            insert_sql = f"""
                INSERT INTO {table_name}
                (doc_id, court_code, judgment_code, justice_kind, category_code,
                 cause_num, adjudication_date, receipt_date, judge, doc_url,
                 status, date_publ, court_name, judgment_name, justice_kind_name,
                 category_name, resolution_text, import_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id) DO NOTHING
            """

        # Підготовляємо значення для executemany
        values_list = []
        for row_data in batch_data:
            values_list.append([
                row_data["doc_id"],
                row_data["court_code"],
                row_data["judgment_code"],
                row_data["justice_kind"],
                row_data["category_code"],
                row_data["cause_num"],
                row_data["adjudication_date"],
                row_data["receipt_date"],
                row_data["judge"],
                row_data["doc_url"],
                row_data["status"],
                row_data["date_publ"],
                row_data["court_name"],
                row_data["judgment_name"],
                row_data["justice_kind_name"],
                row_data["category_name"],
                row_data["resolution_text"],
                row_data["import_date"]
            ])

        # Виконуємо batch insert через executemany
        try:
            with connection.cursor() as cursor:
                cursor.executemany(insert_sql, values_list)
                inserted_count = cursor.rowcount
                return inserted_count
        except Exception as e:
            logger.error(f"Помилка batch вставки: {e}")
            return 0
    def _fast_import_via_copy(self, csv_path, year, total_records):
        """Ультрашвидкий імпорт через PostgreSQL COPY"""
        from django.db import connection
        import tempfile
        import time
        
        table_name = f"court_decisions_{year}"
        start_time = time.time()
        
        self.stdout.write("Створення тимчасової таблиці для COPY...")
        
        temp_table = f"{table_name}_temp"
        
        try:
            with connection.cursor() as cursor:
                # 1. Створюємо тимчасову таблицю БЕЗ обмежень та індексів
                cursor.execute(f"""
                    CREATE TEMP TABLE {temp_table} (
                        doc_id VARCHAR(50),
                        court_code VARCHAR(20),
                        judgment_code VARCHAR(10),
                        justice_kind VARCHAR(10),
                        category_code VARCHAR(20),
                        cause_num VARCHAR(255),
                        adjudication_date VARCHAR(50),
                        receipt_date VARCHAR(50),
                        judge VARCHAR(500),
                        doc_url TEXT,
                        status VARCHAR(10),
                        date_publ VARCHAR(50)
                    )
                """)
                
                self.stdout.write(f"Завантаження даних через COPY з {csv_path}...")
                
                # 2. COPY безпосередньо з CSV файлу - НАЙШВИДШИЙ МЕТОД!
                with open(csv_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(f"""
                        COPY {temp_table} (
                            doc_id, court_code, judgment_code, justice_kind,
                            category_code, cause_num, adjudication_date, receipt_date,
                            judge, doc_url, status, date_publ
                        )
                        FROM STDIN WITH (
                            FORMAT CSV,
                            DELIMITER E'\\t',
                            HEADER TRUE,
                            NULL ''
                        )
                    """, f)
                
                # Перевіряємо скільки завантажилося
                cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                loaded_count = cursor.fetchone()[0]
                self.stdout.write(f"Завантажено {loaded_count} записів за {time.time() - start_time:.1f} сек")
                
                # 3. Вставляємо з тимчасової таблиці в основну (з обробкою дублікатів)
                self.stdout.write("Перенесення даних в основну таблицю...")
                
                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        doc_id, court_code, judgment_code, justice_kind, category_code,
                        cause_num, adjudication_date, receipt_date, judge, doc_url,
                        status, date_publ, court_name, judgment_name, justice_kind_name,
                        category_name, resolution_text, import_date
                    )
                    SELECT 
                        doc_id,
                        court_code,
                        judgment_code,
                        justice_kind,
                        category_code,
                        cause_num,
                        CASE 
                            WHEN adjudication_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN adjudication_date::TIMESTAMP
                            ELSE NULL
                        END,
                        CASE 
                            WHEN receipt_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN receipt_date::TIMESTAMP
                            ELSE NULL
                        END,
                        judge,
                        doc_url,
                        status,
                        CASE 
                            WHEN date_publ ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN date_publ::TIMESTAMP
                            ELSE NULL
                        END,
                        '',  -- court_name
                        '',  -- judgment_name
                        '',  -- justice_kind_name
                        '',  -- category_name
                        '',  -- resolution_text
                        NOW()  -- import_date
                    FROM {temp_table}
                    ON CONFLICT (doc_id) DO NOTHING
                """)
                
                inserted_count = cursor.rowcount
                
                # 4. Видаляємо тимчасову таблицю
                cursor.execute(f"DROP TABLE {temp_table}")
                
                elapsed = time.time() - start_time
                rate = loaded_count / elapsed if elapsed > 0 else 0
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"COPY імпорт завершено за {elapsed:.1f} сек "
                        f"({rate:.0f} записів/сек), вставлено {inserted_count} нових"
                    )
                )
                
                return inserted_count
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка COPY імпорту: {e}"))
            logger.error(f"Помилка COPY імпорту: {e}", exc_info=True)
            
            # Видаляємо тимчасову таблицю при помилці
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            
            return 0
