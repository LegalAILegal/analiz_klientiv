import os
import csv
import logging
import threading
import time
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction, connection
from django.utils import timezone
from concurrent.futures import ThreadPoolExecutor
import queue

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Швидкий імпорт судових рішень з CSV файлів (багатопоточний)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--year", 
            type=int, 
            help="Рік для імпорту (наприклад, 2024)",
            required=True
        )
        parser.add_argument(
            "--threads", 
            type=int, 
            default=8,  # Збільшено за замовчуванням
            help="Кількість потоків для обробки"
        )
        parser.add_argument(
            "--batch-size", 
            type=int, 
            default=50000,  # Збільшено для максимальної швидкості
            help="Розмір батчу для bulk_create"
        )
        parser.add_argument(
            "--chunk-size", 
            type=int, 
            default=200000,  # Збільшено для меншої кількості I/O операцій
            help="Розмір чанка для читання CSV"
        )

    def handle(self, *args, **options):
        year = options["year"]
        threads = options["threads"]
        batch_size = options["batch_size"]
        chunk_size = options["chunk_size"]
        
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
        
        self.stdout.write(f"🚀 ШВИДКИЙ ІМПОРТ судових рішень за {year} рік")
        self.stdout.write(f"📁 Файл: {csv_filename}")
        self.stdout.write(f"🔧 Потоків: {threads}, Батч: {batch_size}, Чанк: {chunk_size}")
        
        # Підрахунок загальної кількості записів
        total_records = self._count_csv_records(csv_path)
        self.stdout.write(f"📊 Всього записів у CSV: {total_records:,}")
        
        # Створення таблиці з оптимізаціями
        table_name = self._create_optimized_table(year)
        
        # Запуск багатопоточного імпорту
        start_time = time.time()
        imported_count = self._parallel_import(
            csv_path, table_name, threads, batch_size, chunk_size, total_records
        )
        end_time = time.time()
        
        duration = end_time - start_time
        records_per_second = imported_count / duration if duration > 0 else 0
        
        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Імпорт завершено за {duration:.1f} сек!\n"
                f"📈 Оброблено: {imported_count:,} записів\n"
                f"⚡ Швидкість: {records_per_second:,.0f} записів/сек"
            )
        )

    def _count_csv_records(self, csv_path):
        """Швидкий підрахунок кількості записів"""
        try:
            with open(csv_path, "rb") as file:
                lines = sum(1 for _ in file) - 1  # Мінус заголовок
                return lines
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка читання CSV: {e}"))
            return 0

    def _create_optimized_table(self, year):
        """Створення оптимізованої таблиці"""
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
            
            if cursor.fetchone()[0]:
                self.stdout.write(f"📋 Таблиця {table_name} вже існує")
                return table_name
            
            # Створюємо таблицю БЕЗ індексів для швидкої вставки
            create_table_sql = f"""
                CREATE UNLOGGED TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    doc_id VARCHAR(50) NOT NULL,
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
            
            cursor.execute(create_table_sql)
            
            # Базові налаштування для швидкої вставки
            try:
                cursor.execute("SET synchronous_commit = off;")
            except:
                pass  # Ігноруємо помилки налаштувань
            
            self.stdout.write(f"🆕 Створено оптимізовану таблицю {table_name}")
        
        return table_name

    def _parallel_import(self, csv_path, table_name, threads, batch_size, chunk_size, total_records):
        """Багатопоточний імпорт"""
        
        # Черга для чанків даних
        data_queue = queue.Queue(maxsize=threads * 2)
        
        # Статистика
        self.imported_count = 0
        self.processed_chunks = 0
        self.lock = threading.Lock()
        
        # Запуск монітору прогресу
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=self._progress_monitor, 
            args=(stop_monitor, total_records)
        )
        monitor_thread.start()
        
        # Запуск воркерів
        with ThreadPoolExecutor(max_workers=threads) as executor:
            # Запускаємо воркери
            futures = []
            for i in range(threads):
                future = executor.submit(
                    self._worker_thread, 
                    data_queue, table_name, batch_size, i
                )
                futures.append(future)
            
            # Читаємо файл чанками і додаємо до черги
            self._read_csv_chunks(csv_path, chunk_size, data_queue)
            
            # Сигналізуємо кінець даних
            for _ in range(threads):
                data_queue.put(None)
            
            # Чекаємо завершення всіх воркерів
            for future in futures:
                future.result()
        
        # Зупиняємо монітор
        stop_monitor.set()
        monitor_thread.join()
        
        # Створюємо індекси після імпорту
        self._create_indexes_after_import(table_name)
        
        return self.imported_count

    def _read_csv_chunks(self, csv_path, chunk_size, data_queue):
        """Читання CSV чанками"""
        try:
            with open(csv_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file, delimiter="\t")
                
                chunk = []
                for row in reader:
                    chunk.append(row)
                    
                    if len(chunk) >= chunk_size:
                        data_queue.put(chunk.copy())
                        chunk = []
                
                # Додаємо останній чанк
                if chunk:
                    data_queue.put(chunk)
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка читання CSV: {e}"))

    def _worker_thread(self, data_queue, table_name, batch_size, worker_id):
        """Воркер для обробки чанків"""
        while True:
            chunk = data_queue.get()
            if chunk is None:
                break
            
            try:
                processed = self._process_chunk(chunk, table_name, batch_size)
                
                with self.lock:
                    self.imported_count += processed
                    self.processed_chunks += 1
                    
            except Exception as e:
                logger.error(f"Помилка в воркері {worker_id}: {e}")
            finally:
                data_queue.task_done()

    def _process_chunk(self, chunk, table_name, batch_size):
        """Обробка чанка даних"""
        processed_count = 0
        batch_data = []
        
        for row in chunk:
            processed_row = self._process_csv_row(row)
            batch_data.append(processed_row)
            
            if len(batch_data) >= batch_size:
                inserted = self._bulk_insert_batch(table_name, batch_data)
                processed_count += inserted
                batch_data = []
        
        # Останній батч
        if batch_data:
            inserted = self._bulk_insert_batch(table_name, batch_data)
            processed_count += inserted
        
        return processed_count

    def _bulk_insert_batch(self, table_name, batch_data):
        """Масова вставка батчу використовуючи COPY"""
        if not batch_data:
            return 0
        
        try:
            with connection.cursor() as cursor:
                # Використовуємо PostgreSQL COPY для максимальної швидкості
                copy_sql = f"""
                    COPY {table_name} 
                    (doc_id, court_code, judgment_code, justice_kind, category_code, 
                     cause_num, adjudication_date, receipt_date, judge, doc_url, 
                     status, date_publ, court_name, judgment_name, justice_kind_name, 
                     category_name, resolution_text, import_date)
                    FROM STDIN WITH CSV
                """
                
                # Підготовлюємо дані для COPY
                import io
                data_io = io.StringIO()
                for row_data in batch_data:
                    values = [
                        self._escape_csv_value(row_data["doc_id"]),
                        self._escape_csv_value(row_data["court_code"]),
                        self._escape_csv_value(row_data["judgment_code"]),
                        self._escape_csv_value(row_data["justice_kind"]),
                        self._escape_csv_value(row_data["category_code"]),
                        self._escape_csv_value(row_data["cause_num"]),
                        row_data["adjudication_date"].isoformat() if row_data["adjudication_date"] else "",
                        row_data["receipt_date"].isoformat() if row_data["receipt_date"] else "",
                        self._escape_csv_value(row_data["judge"]),
                        self._escape_csv_value(row_data["doc_url"]),
                        self._escape_csv_value(row_data["status"]),
                        row_data["date_publ"].isoformat() if row_data["date_publ"] else "",
                        self._escape_csv_value(row_data["court_name"]),
                        self._escape_csv_value(row_data["judgment_name"]),
                        self._escape_csv_value(row_data["justice_kind_name"]),
                        self._escape_csv_value(row_data["category_name"]),
                        self._escape_csv_value(row_data["resolution_text"]),
                        row_data["import_date"].isoformat()
                    ]
                    data_io.write(",".join(values) + "\n")
                
                data_io.seek(0)
                cursor.copy_expert(copy_sql, data_io)
                
                return len(batch_data)
                
        except Exception as e:
            logger.error(f"Помилка bulk insert: {e}")
            return 0

    def _escape_csv_value(self, value):
        """Екранування значень для CSV"""
        if value is None:
            return ""
        
        value_str = str(value).replace(""", """")  # Екранування лапок
        if "," in value_str or "\n" in value_str or """ in str(value):
            return f""{value_str}""
        
        return value_str

    def _process_csv_row(self, row):
        """Обробка одного рядка CSV"""
        def parse_date(date_str):
            if not date_str or date_str.strip() == "":
                return None
            
            try:
                # Формат: "2024-01-01 00:00:00+02"
                if "+" in date_str or "-" in date_str[-6:]:
                    date_part = date_str.split("+")[0].split(" ")[0] if "+" in date_str else date_str.split("-")[0]
                    return datetime.strptime(date_part, "%Y-%m-%d")
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
            "court_name": "",
            "judgment_name": "",
            "justice_kind_name": "",
            "category_name": "",
            "resolution_text": "",
            "import_date": timezone.now()
        }

    def _progress_monitor(self, stop_event, total_records):
        """Монітор прогресу кожні 10 секунд"""
        start_time = time.time()
        
        while not stop_event.wait(10):  # Чекаємо 10 секунд або stop_event
            with self.lock:
                current_count = self.imported_count
                chunks = self.processed_chunks
            
            elapsed = time.time() - start_time
            progress_percent = (current_count / total_records * 100) if total_records > 0 else 0
            records_per_sec = current_count / elapsed if elapsed > 0 else 0
            
            self.stdout.write(
                f"⚡ {current_count:,} записів ({progress_percent:.1f}%) | "
                f"{records_per_sec:,.0f} зап/сек | {chunks} чанків | "
                f"{elapsed:.0f} сек"
            )

    def _create_indexes_after_import(self, table_name):
        """Створення індексів після імпорту"""
        self.stdout.write("🔧 Створення індексів...")
        
        with connection.cursor() as cursor:
            indexes_sql = [
                f"CREATE UNIQUE INDEX idx_{table_name}_doc_id ON {table_name} (doc_id);",
                f"CREATE INDEX idx_{table_name}_cause_num ON {table_name} USING btree (cause_num);",
                f"CREATE INDEX idx_{table_name}_court_code ON {table_name} USING btree (court_code);",
                f"CREATE INDEX idx_{table_name}_adjudication_date ON {table_name} USING btree (adjudication_date);",
                f"CREATE INDEX idx_{table_name}_case_search ON {table_name} USING btree (cause_num, court_code, adjudication_date);",
            ]
            
            for index_sql in indexes_sql:
                try:
                    cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Не вдалося створити індекс: {e}")
            
            # Повертаємо таблицю до LOGGED режиму та включаємо synchronous_commit
            cursor.execute(f"ALTER TABLE {table_name} SET LOGGED;")
            cursor.execute("SET synchronous_commit = on;")
        
        self.stdout.write("✅ Індекси створено!")