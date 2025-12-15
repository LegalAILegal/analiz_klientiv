import csv
import os
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from bankruptcy.models import BankruptcyCase, Company, Court, TrackedBankruptcyCase


class Command(BaseCommand):
    help = "Завантажує дані про справи банкрутства з CSV файлу"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            default="data/Відомості про справи про банкрутство.csv",
            help="Шлях до CSV файлу"
        )
        parser.add_argument(
            "--incremental",
            action="store_true",
            help="Інкрементальне оновлення (тільки нові записи)"
        )

    def handle(self, *args, **options):
        file_path = options["file"]
        incremental = options["incremental"]
        
        if not os.path.exists(file_path):
            raise CommandError(f"Файл '{file_path}' не знайдено.")

        self.stdout.write(
            self.style.SUCCESS(f"Початок завантаження даних з {file_path}")
        )

        try:
            with open(file_path, "r", encoding="utf-8") as csvfile:
                if incremental:
                    self.load_incremental(csvfile)
                else:
                    self.load_full(csvfile)
        except Exception as e:
            raise CommandError(f"Помилка при завантаженні даних: {str(e)}")

    def load_full(self, csvfile):
        """Повне завантаження даних (видаляє попередні)"""
        self.stdout.write("Виконується повне завантаження...")
        
        with transaction.atomic():
            BankruptcyCase.objects.all().delete()
            Company.objects.all().delete()
            Court.objects.all().delete()
            
            self._process_csv(csvfile)

    def load_incremental(self, csvfile):
        """Інкрементальне завантаження (тільки нові записи)"""
        self.stdout.write("Виконується інкрементальне завантаження...")
        
        existing_numbers = set(
            BankruptcyCase.objects.values_list("number", flat=True)
        )
        
        reader = csv.DictReader(csvfile, delimiter="\t")
        new_records = 0
        new_tracked = 0
        
        for row_num, row in enumerate(reader, 1):
            try:
                number = int(row["number"].strip('"'))
                if number not in existing_numbers:
                    tracked_created = self._process_row(row)
                    new_records += 1
                    if tracked_created:
                        new_tracked += 1
                    
                    if row_num % 1000 == 0:
                        self.stdout.write(f"Оброблено {row_num} рядків...")
                        
            except Exception as e:
                self.stdout.write(
                    self.style.WARNING(
                        f"Помилка у рядку {row_num}: {str(e)}"
                    )
                )
        
        self.stdout.write(
            self.style.SUCCESS(f"Додано {new_records} нових записів")
        )
        if new_tracked > 0:
            self.stdout.write(
                self.style.SUCCESS(f"➕ Автоматично додано до відстеження: {new_tracked} справ")
            )

    def _process_csv(self, csvfile):
        """Обробка CSV файлу"""
        reader = csv.DictReader(csvfile, delimiter="\t")
        processed = 0
        
        for row_num, row in enumerate(reader, 1):
            try:
                self._process_row(row)
                processed += 1
                
                if row_num % 1000 == 0:
                    self.stdout.write(f"Оброблено {row_num} рядків...")
                    
            except Exception as e:
                self.stdout.write(
                    self.style.WARNING(
                        f"Помилка у рядку {row_num}: {str(e)}"
                    )
                )
        
        self.stdout.write(
            self.style.SUCCESS(f"Успішно оброблено {processed} записів")
        )

    def _process_row(self, row):
        """Обробка одного рядка CSV"""
        # Парсинг даних
        number = int(row["number"].strip('"'))
        date_str = row["date"].strip('"')
        type_str = row["type"].strip('"')
        
        # Фільтрація небажаних типів проваджень
        excluded_types = [
            "Оголошення про проведення аукціону з продажу майна",
            "Повідомлення про результати проведення аукціону з продажу майна", 
            "Повідомлення про скасування аукціону з продажу майна"
        ]
        
        if type_str in excluded_types:
            return  # Пропускаємо цей запис
        
        firm_edrpou = row["firm_edrpou"].strip('"')
        firm_name = row["firm_name"].strip('"').strip()
        case_number = row["case_number"].strip('"')
        start_date_auc = row["start_date_auc"].strip('"')
        end_date_auc = row["end_date_auc"].strip('"')
        court_name = row["court_name"].strip('"')
        end_registration_date = row["end_registration_date"].strip('"')

        # Парсинг дат
        date = datetime.strptime(date_str, "%d.%m.%Y").date()
        start_date_auc_obj = None
        end_date_auc_obj = None
        end_registration_date_obj = None

        if start_date_auc:
            start_date_auc_obj = datetime.strptime(start_date_auc, "%d.%m.%Y").date()
        
        if end_date_auc:
            end_date_auc_obj = datetime.strptime(end_date_auc, "%d.%m.%Y").date()
        
        if end_registration_date:
            end_registration_date_obj = datetime.strptime(end_registration_date, "%d.%m.%Y").date()

        # Створення або отримання об"єктів
        court, _ = Court.objects.get_or_create(name=court_name)
        company, _ = Company.objects.get_or_create(
            edrpou=firm_edrpou,
            defaults={"name": firm_name}
        )

        # Створення справи банкрутства
        bankruptcy_case, case_created = BankruptcyCase.objects.update_or_create(
            number=number,
            defaults={
                "date": date,
                "type": type_str,
                "company": company,
                "case_number": case_number,
                "start_date_auc": start_date_auc_obj,
                "end_date_auc": end_date_auc_obj,
                "court": court,
                "end_registration_date": end_registration_date_obj,
            }
        )
        
        # Автоматично створюємо запис для відстеження якщо справа нова або немає запису
        tracked_created = False
        if case_number and case_number.strip():  # Тільки якщо є номер справи
            tracked_case, tracked_created = TrackedBankruptcyCase.objects.get_or_create(
                bankruptcy_case=bankruptcy_case,
                defaults={
                    "status": "active",
                    "priority": 1,
                    "search_decisions_status": "pending"
                }
            )
            if tracked_created:
                self.stdout.write(f"➕ Додано до відстеження справу: {case_number}")
        
        return tracked_created  # Повертаємо чи було створено новий запис для відстеження