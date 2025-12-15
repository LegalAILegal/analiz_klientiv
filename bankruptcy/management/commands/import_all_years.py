import os
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from django.core.management import call_command

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Автоматичний імпорт судових рішень для всіх років (2014-2023)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--start-year", 
            type=int, 
            default=2014,
            help="Рік початку імпорту (за замовчуванням: 2014)"
        )
        parser.add_argument(
            "--end-year", 
            type=int, 
            default=2023,
            help="Рік кінця імпорту (за замовчуванням: 2023)"
        )
        parser.add_argument(
            "--threads", 
            type=int, 
            default=6,
            help="Кількість потоків для кожного імпорту"
        )
        parser.add_argument(
            "--batch-size", 
            type=int, 
            default=25000,
            help="Розмір батчу для bulk_create"
        )
        parser.add_argument(
            "--chunk-size", 
            type=int, 
            default=150000,
            help="Розмір чанка для читання CSV"
        )
        parser.add_argument(
            "--skip-existing",
            action="store_true",
            help="Пропустити роки з існуючими таблицями"
        )

    def handle(self, *args, **options):
        start_year = options["start_year"]
        end_year = options["end_year"]
        threads = options["threads"]
        batch_size = options["batch_size"]
        chunk_size = options["chunk_size"]
        skip_existing = options["skip_existing"]
        
        self.stdout.write(f"🚀 АВТОМАТИЧНИЙ ІМПОРТ судових рішень за {start_year}-{end_year} роки")
        self.stdout.write(f"⚙️ Параметри: потоків={threads}, батч={batch_size}, чанк={chunk_size}")
        
        # Перевіряємо наявні CSV файли
        available_years = self._check_available_files(start_year, end_year)
        
        if not available_years:
            self.stdout.write(self.style.ERROR("❌ Не знайдено жодного CSV файлу для імпорту"))
            return
        
        self.stdout.write(f"📁 Знайдено CSV файлів: {len(available_years)} ({", ".join(map(str, available_years))})")
        
        # Перевіряємо існуючі таблиці
        if skip_existing:
            existing_tables = self._check_existing_tables(available_years)
            if existing_tables:
                self.stdout.write(f"⏭️ Пропускаємо існуючі таблиці: {", ".join(existing_tables)}")
                available_years = [year for year in available_years 
                                 if f"court_decisions_{year}" not in existing_tables]
        
        if not available_years:
            self.stdout.write(self.style.WARNING("⚠️ Всі таблиці вже існують"))
            return
        
        total_success = 0
        total_failed = 0
        
        # Імпортуємо кожен рік
        for year in sorted(available_years):
            self.stdout.write(f"\n" + "="*50)
            self.stdout.write(f"🔄 Імпорт судових рішень за {year} рік")
            self.stdout.write("="*50)
            
            try:
                # Викликаємо швидку команду імпорту
                call_command(
                    "import_court_decisions_fast",
                    year=year,
                    threads=threads,
                    batch_size=batch_size,
                    chunk_size=chunk_size
                )
                
                total_success += 1
                self.stdout.write(self.style.SUCCESS(f"✅ {year} рік - УСПІШНО"))
                
            except Exception as e:
                total_failed += 1
                self.stdout.write(self.style.ERROR(f"❌ {year} рік - ПОМИЛКА: {str(e)}"))
                logger.error(f"Помилка імпорту {year} року: {e}")
        
        # Підсумок
        self.stdout.write(f"\n" + "="*50)
        self.stdout.write(f"📊 ПІДСУМОК ІМПОРТУ")
        self.stdout.write("="*50)
        self.stdout.write(f"✅ Успішно: {total_success} років")
        self.stdout.write(f"❌ Помилки: {total_failed} років")
        self.stdout.write(f"📈 Загальний успіх: {total_success}/{total_success + total_failed}")
        
        if total_success > 0:
            self.stdout.write("\n🎯 Створюємо підсумкову статистику...")
            self._show_final_statistics()

    def _check_available_files(self, start_year, end_year):
        """Перевіряє наявні CSV файли"""
        available_years = []
        
        for year in range(start_year, end_year + 1):
            if year >= 2000:
                short_year = year - 2000
            else:
                short_year = year - 1900
            
            csv_filename = f"documents_{short_year:02d}.csv"
            csv_path = os.path.join(settings.BASE_DIR, "data", csv_filename)
            
            if os.path.exists(csv_path):
                file_size = os.path.getsize(csv_path)
                size_mb = file_size / (1024 * 1024)
                available_years.append(year)
                self.stdout.write(f"📄 {year}: {csv_filename} ({size_mb:.1f} МБ)")
            else:
                self.stdout.write(f"❌ {year}: {csv_filename} - НЕ ЗНАЙДЕНО")
        
        return available_years

    def _check_existing_tables(self, years):
        """Перевіряє існуючі таблиці"""
        from django.db import connection
        
        existing_tables = []
        
        with connection.cursor() as cursor:
            for year in years:
                table_name = f"court_decisions_{year}"
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, [table_name])
                
                if cursor.fetchone()[0]:
                    existing_tables.append(table_name)
        
        return existing_tables

    def _show_final_statistics(self):
        """Показує підсумкову статистику по всіх таблицях"""
        from django.db import connection
        
        try:
            with connection.cursor() as cursor:
                # Отримуємо всі таблиці судових рішень
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'court_decisions_%"
                    ORDER BY table_name;
                """)
                
                tables = [row[0] for row in cursor.fetchall()]
                
                if not tables:
                    return
                
                self.stdout.write(f"\n📊 СТАТИСТИКА ПО ВСІХ ТАБЛИЦЯХ ({len(tables)} таблиць):")
                self.stdout.write("-" * 70)
                
                total_records = 0
                total_size = 0
                
                for table_name in tables:
                    try:
                        # Кількість записів
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                        record_count = cursor.fetchone()[0]
                        
                        # Розмір таблиці
                        cursor.execute(f"""
                            SELECT pg_size_pretty(pg_total_relation_size("{table_name}")) as size,
                                   pg_total_relation_size("{table_name}") as size_bytes;
                        """)
                        size_pretty, size_bytes = cursor.fetchone()
                        
                        year = table_name.replace("court_decisions_", "")
                        
                        self.stdout.write(f"{year:>4} | {record_count:>10,} записів | {size_pretty:>8}")
                        
                        total_records += record_count
                        total_size += size_bytes
                        
                    except Exception as e:
                        self.stdout.write(f"{table_name} | ПОМИЛКА: {e}")
                
                self.stdout.write("-" * 70)
                total_size_gb = total_size / (1024 ** 3)
                self.stdout.write(f"{"ВСЬОГО":>4} | {total_records:>10,} записів | {total_size_gb:.1f} ГБ")
                
                self.stdout.write(f"\n🎯 БАЗА ДАНИХ СУДОВИХ РІШЕНЬ ГОТОВА!")
                self.stdout.write(f"💾 Загальний розмір: {total_size_gb:.1f} ГБ")
                self.stdout.write(f"📈 Загальна кількість рішень: {total_records:,}")
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка отримання статистики: {e}"))