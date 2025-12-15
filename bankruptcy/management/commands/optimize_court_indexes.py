from django.core.management.base import BaseCommand
from django.db import connection, transaction
import logging
import time

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Створює оптимізовані індекси для швидкого точного пошуку судових рішень (адаптація SR_AI підходу)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--drop-existing",
            action="store_true",
            help="Видалити існуючі індекси перед створенням нових",
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Оптимізувати індекси тільки для конкретного року",
        )
        parser.add_argument(
            "--analyze",
            action="store_true",
            help="Виконати ANALYZE після створення індексів",
        )

    def handle(self, *args, **options):
        drop_existing = options["drop_existing"]
        target_year = options["year"]
        analyze_after = options["analyze"]

        self.stdout.write("🚀 Початок оптимізації індексів для швидкого пошуку судових рішень")
        self.stdout.write("📋 Базується на принципах SR_AI з адаптацією для точного пошуку")

        try:
            # Отримуємо список усіх таблиць судових рішень
            tables = self._get_court_decision_tables(target_year)
            
            if not tables:
                self.stdout.write("⚠️ Таблиці судових рішень не знайдено")
                return

            self.stdout.write(f"📊 Знайдено {len(tables)} таблиць для оптимізації: {", ".join(tables)}")

            total_time = 0
            optimized_count = 0

            for table in tables:
                self.stdout.write(f"\n🔧 Оптимізація таблиці: {table}")
                
                start_time = time.time()
                success = self._optimize_table(table, drop_existing)
                end_time = time.time()
                
                if success:
                    optimized_count += 1
                    elapsed = end_time - start_time
                    total_time += elapsed
                    self.stdout.write(
                        self.style.SUCCESS(f"✅ {table} оптимізовано за {elapsed:.2f}с")
                    )
                else:
                    self.stdout.write(
                        self.style.ERROR(f"❌ Помилка оптимізації {table}")
                    )

            # Виконуємо ANALYZE якщо потрібно
            if analyze_after and optimized_count > 0:
                self.stdout.write("\n📈 Виконання ANALYZE для оптимізації планувальника запитів...")
                self._analyze_tables(tables)

            # Підсумки
            self.stdout.write("\n" + "="*60)
            self.stdout.write("📊 ПІДСУМКИ ОПТИМІЗАЦІЇ:")
            self.stdout.write(f"✅ Оптимізовано таблиць: {optimized_count}/{len(tables)}")
            self.stdout.write(f"⏱️ Загальний час: {total_time:.2f} секунд")
            
            if optimized_count > 0:
                self.stdout.write("\n🎯 СТВОРЕНІ ІНДЕКСИ ДЛЯ ШВИДКОГО ТОЧНОГО ПОШУКУ:")
                self.stdout.write("• Hash індекс для точного пошуку номера справи")
                self.stdout.write("• B-tree індекс для діапазонних запитів")
                self.stdout.write("• Композитний індекс для сортування")
                self.stdout.write("\n🚀 Очікуване прискорення: 10-50x для точного пошуку")

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"💥 Критична помилка: {e}")
            )
            logger.error(f"Помилка команди optimize_court_indexes: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")

    def _get_court_decision_tables(self, target_year=None):
        """Отримання списку таблиць судових рішень"""
        with connection.cursor() as cursor:
            if target_year:
                # Шукаємо тільки таблицю конкретного року
                table_pattern = f"court_decisions_{target_year}"
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
                    AND table_name LIKE 'court_decisions_%"
                    ORDER BY table_name DESC
                """)
            
            return [row[0] for row in cursor.fetchall()]

    def _optimize_table(self, table_name, drop_existing=False):
        """Оптимізація індексів для конкретної таблиці"""
        try:
            with connection.cursor() as cursor:
                # Перевіряємо структуру таблиці
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND table_schema = 'public'
                """, [table_name])
                
                columns = [row[0] for row in cursor.fetchall()]
                required_columns = ["cause_num", "doc_id", "court_code"]
                
                # Перевіряємо наявність необхідних колонок
                missing_columns = [col for col in required_columns if col not in columns]
                if missing_columns:
                    self.stdout.write(
                        self.style.WARNING(
                            f"⚠️ У таблиці {table_name} відсутні колонки: {missing_columns}"
                        )
                    )

                # Видаляємо старі індекси якщо потрібно
                if drop_existing:
                    self._drop_existing_indexes(cursor, table_name)

                # Створюємо оптимізовані індекси (адаптація SR_AI принципів)
                indexes_created = 0

                # 1. HASH індекс для точного пошуку cause_num (найшвидший для = запитів)
                if "cause_num" in columns:
                    try:
                        cursor.execute(f"""
                            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table_name}_cause_num_hash 
                            ON {table_name} USING hash (cause_num)
                        """)
                        indexes_created += 1
                        self.stdout.write(f"  ✅ Hash індекс для cause_num створено")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ Hash індекс: {e}")

                # 2. B-tree індекс для cause_num (для діапазонних запитів та сортування)
                if "cause_num" in columns:
                    try:
                        cursor.execute(f"""
                            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table_name}_cause_num_btree 
                            ON {table_name} (cause_num)
                        """)
                        indexes_created += 1
                        self.stdout.write(f"  ✅ B-tree індекс для cause_num створено")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ B-tree індекс: {e}")

                # 3. Hash індекс для doc_id (унікальний ідентифікатор)
                if "doc_id" in columns:
                    try:
                        cursor.execute(f"""
                            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table_name}_doc_id_hash 
                            ON {table_name} USING hash (doc_id)
                        """)
                        indexes_created += 1
                        self.stdout.write(f"  ✅ Hash індекс для doc_id створено")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ Doc_id hash індекс: {e}")

                # 4. Композитний індекс для швидкого пошуку + сортування
                if all(col in columns for col in ["cause_num", "adjudication_date"]):
                    try:
                        cursor.execute(f"""
                            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table_name}_cause_date 
                            ON {table_name} (cause_num, adjudication_date DESC)
                        """)
                        indexes_created += 1
                        self.stdout.write(f"  ✅ Композитний індекс cause_num + date створено")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ Композитний індекс: {e}")

                # 5. Індекс для court_code (для фільтрації по судах)
                if "court_code" in columns:
                    try:
                        cursor.execute(f"""
                            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{table_name}_court_code 
                            ON {table_name} (court_code)
                        """)
                        indexes_created += 1
                        self.stdout.write(f"  ✅ Індекс для court_code створено")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ Court_code індекс: {e}")

                self.stdout.write(f"  📊 Створено {indexes_created} індексів для {table_name}")
                return True

        except Exception as e:
            logger.error(f"Помилка оптимізації таблиці {table_name}: {e}")
            return False

    def _drop_existing_indexes(self, cursor, table_name):
        """Видалення існуючих індексів для таблиці"""
        try:
            # Отримуємо список індексів для таблиці
            cursor.execute("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = %s 
                AND schemaname = "public"
                AND indexname LIKE %s
            """, [table_name, f"idx_{table_name}_%"])
            
            indexes = [row[0] for row in cursor.fetchall()]
            
            if indexes:
                self.stdout.write(f"  🗑️ Видалення {len(indexes)} існуючих індексів...")
                for index_name in indexes:
                    try:
                        cursor.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
                        self.stdout.write(f"    ✅ Видалено: {index_name}")
                    except Exception as e:
                        self.stdout.write(f"    ⚠️ Помилка видалення {index_name}: {e}")

        except Exception as e:
            self.stdout.write(f"  ⚠️ Помилка видалення індексів: {e}")

    def _analyze_tables(self, tables):
        """Виконання ANALYZE для таблиць"""
        try:
            with connection.cursor() as cursor:
                for table in tables:
                    try:
                        cursor.execute(f"ANALYZE {table}")
                        self.stdout.write(f"  ✅ ANALYZE виконано для {table}")
                    except Exception as e:
                        self.stdout.write(f"  ⚠️ Помилка ANALYZE для {table}: {e}")
                        
        except Exception as e:
            self.stdout.write(f"⚠️ Помилка виконання ANALYZE: {e}")

    def _show_index_info(self, table_name):
        """Показати інформацію про індекси таблиці"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        indexname,
                        indexdef
                    FROM pg_indexes 
                    WHERE tablename = %s 
                    AND schemaname = "public"
                    ORDER BY indexname
                """, [table_name])
                
                indexes = cursor.fetchall()
                if indexes:
                    self.stdout.write(f"\n📋 Індекси таблиці {table_name}:")
                    for idx_name, idx_def in indexes:
                        self.stdout.write(f"  • {idx_name}")
                        
        except Exception as e:
            self.stdout.write(f"⚠️ Помилка отримання інформації про індекси: {e}")