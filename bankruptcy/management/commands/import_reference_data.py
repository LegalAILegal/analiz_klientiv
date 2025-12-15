import os
import csv
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Імпорт довідкових даних з CSV файлів"

    def add_arguments(self, parser):
        parser.add_argument(
            "--force", 
            action="store_true",
            help="Перезаписати існуючі записи"
        )

    def handle(self, *args, **options):
        force = options["force"]
        
        # Список довідкових файлів та відповідних таблиць
        reference_files = [
            {
                "file": "regions.csv",
                "table": "bankruptcy_regions",
                "columns": ["code", "name"],
                "description": "Регіони"
            },
            {
                "file": "courts.csv", 
                "table": "bankruptcy_courts_ref",
                "columns": ["court_code", "name", "instance_code", "region_code"],
                "description": "Суди"
            },
            {
                "file": "instances.csv",
                "table": "bankruptcy_instances",
                "columns": ["code", "name"],
                "description": "Інстанції"
            },
            {
                "file": "justice_kinds.csv",
                "table": "bankruptcy_justice_kinds",
                "columns": ["code", "name"],
                "description": "Види судочинства"
            },
            {
                "file": "judgment_forms.csv",
                "table": "bankruptcy_judgment_forms",
                "columns": ["code", "name"],
                "description": "Форми судових рішень"
            },
            {
                "file": "cause_categories.csv",
                "table": "bankruptcy_cause_categories",
                "columns": ["code", "name"],
                "description": "Категорії справ"
            }
        ]
        
        for ref_file in reference_files:
            self.stdout.write(f"\n--- Імпорт {ref_file["description"]} з файлу {ref_file["file"]} ---")
            self._import_reference_file(ref_file, force)
        
        self.stdout.write(self.style.SUCCESS("\nІмпорт довідкових даних завершено"))

    def _import_reference_file(self, ref_config, force):
        """Імпорт одного довідкового файлу"""
        csv_path = os.path.join(settings.BASE_DIR, "data", ref_config["file"])
        
        if not os.path.exists(csv_path):
            self.stdout.write(
                self.style.WARNING(f"Файл {ref_config["file"]} не знайдено, пропускаємо")
            )
            return
        
        # Створюємо таблицю якщо не існує
        self._create_reference_table(ref_config)
        
        # Імпортуємо дані
        imported_count = self._import_csv_to_table(csv_path, ref_config, force)
        
        self.stdout.write(f"Імпортовано {imported_count} записів до {ref_config["table"]}")

    def _create_reference_table(self, ref_config):
        """Створення довідкової таблиці якщо не існує"""
        table_name = ref_config["table"]
        columns = ref_config["columns"]
        
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
                return  # Таблиця вже існує
            
            # Створюємо таблицю залежно від кількості стовпців
            if len(columns) == 2:
                # Проста таблиця: code, name
                create_sql = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        code VARCHAR(50) UNIQUE NOT NULL,
                        name VARCHAR(500) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """
            elif table_name == "bankruptcy_courts_ref":
                # Спеціальна таблиця для судів
                create_sql = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        court_code VARCHAR(50) UNIQUE NOT NULL,
                        name VARCHAR(500) NOT NULL,
                        instance_code VARCHAR(50),
                        region_code VARCHAR(50),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """
            else:
                # Загальний випадок
                columns_def = ", ".join([f"{col} VARCHAR(500)" for col in columns])
                create_sql = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        {columns_def},
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """
            
            cursor.execute(create_sql)
            
            # Створюємо індекс на код
            if "code" in columns:
                cursor.execute(f"CREATE INDEX idx_{table_name}_code ON {table_name} (code);")
            elif "court_code" in columns:
                cursor.execute(f"CREATE INDEX idx_{table_name}_court_code ON {table_name} (court_code);")
            
            self.stdout.write(f"Створено таблицю {table_name}")

    def _import_csv_to_table(self, csv_path, ref_config, force):
        """Імпорт CSV файлу до таблиці"""
        table_name = ref_config["table"]
        columns = ref_config["columns"]
        imported_count = 0
        
        try:
            with open(csv_path, "r", encoding="utf-8") as file:
                # Автоматично визначаємо роздільник
                sample = file.read(1024)
                file.seek(0)
                
                delimiter = "\t" if "\t" in sample else ","
                reader = csv.DictReader(file, delimiter=delimiter)
                
                with connection.cursor() as cursor:
                    for row in reader:
                        try:
                            # Підготовлюємо дані для вставки
                            if table_name == "bankruptcy_courts_ref":
                                values = [
                                    row.get("court_code", "").strip(),
                                    row.get("name", "").strip().strip("""),
                                    row.get("instance_code", "").strip(),
                                    row.get("region_code", "").strip()
                                ]
                                unique_column = "court_code"
                            else:
                                # Для простих довідників
                                values = [
                                    row.get("code", "").strip(),
                                    row.get("name", "").strip().strip(""")
                                ]
                                unique_column = "code"
                            
                            # Перевіряємо чи запис існує
                            if not force:
                                cursor.execute(
                                    f"SELECT 1 FROM {table_name} WHERE {unique_column} = %s", 
                                    [values[0]]
                                )
                                if cursor.fetchone():
                                    continue  # Пропускаємо існуючий запис
                            
                            # Вставляємо запис
                            if table_name == "bankruptcy_courts_ref":
                                placeholders = "%s, %s, %s, %s"
                                columns_list = "court_code, name, instance_code, region_code"
                                conflict_update = """
                                    name = EXCLUDED.name,
                                    instance_code = EXCLUDED.instance_code,
                                    region_code = EXCLUDED.region_code
                                """ if force else "court_code = EXCLUDED.court_code"
                            else:
                                placeholders = "%s, %s"
                                columns_list = "code, name"
                                conflict_update = "name = EXCLUDED.name" if force else "code = EXCLUDED.code"
                            
                            insert_sql = f"""
                                INSERT INTO {table_name} ({columns_list})
                                VALUES ({placeholders})
                                ON CONFLICT ({unique_column}) 
                                DO {"UPDATE" if force else "NOTHING"} SET {conflict_update if force else ""}
                            """
                            
                            cursor.execute(insert_sql, values)
                            imported_count += 1
                            
                        except Exception as e:
                            logger.error(f"Помилка вставки запису в {table_name}: {e}")
                            continue
                            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Помилка читання файлу {csv_path}: {e}"))
            return imported_count
        
        return imported_count