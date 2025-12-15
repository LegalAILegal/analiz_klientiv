import logging
from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.models import BankruptcyCase

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Пов\"язання судових рішень зі справами банкрутства"

    def add_arguments(self, parser):
        parser.add_argument(
            "--case-number", 
            type=str, 
            help="Номер справи для пошуку (опційно, для конкретної справи)"
        )
        parser.add_argument(
            "--limit", 
            type=int, 
            default=100,
            help="Максимальна кількість справ для обробки"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Тільки показати результати без збереження"
        )

    def handle(self, *args, **options):
        case_number = options.get("case_number")
        limit = options["limit"]
        dry_run = options["dry_run"]
        
        if dry_run:
            self.stdout.write(self.style.WARNING("Режим тестування - зміни не будуть збережені"))
        
        # Отримуємо справи банкрутства для обробки
        if case_number:
            bankruptcy_cases = BankruptcyCase.objects.filter(case_number=case_number)
            self.stdout.write(f"Пошук судових рішень для справи: {case_number}")
        else:
            bankruptcy_cases = BankruptcyCase.objects.all()[:limit]
            self.stdout.write(f"Пошук судових рішень для {len(bankruptcy_cases)} справ банкрутства")
        
        # Отримуємо список доступних таблиць судових рішень
        court_decision_tables = self._get_court_decision_tables()
        self.stdout.write(f"Доступні таблиці судових рішень: {len(court_decision_tables)}")
        
        total_found = 0
        total_processed = 0
        
        for bankruptcy_case in bankruptcy_cases:
            total_processed += 1
            
            # Шукаємо судові рішення для цієї справи
            decisions_found = self._search_decisions_for_case(
                bankruptcy_case, court_decision_tables
            )
            
            if decisions_found:
                total_found += len(decisions_found)
                self.stdout.write(
                    f"Справа {bankruptcy_case.case_number}: знайдено {len(decisions_found)} рішень"
                )
                
                if not dry_run:
                    # Зберігаємо результати (можна додати логіку збереження)
                    pass
            
            # Показуємо прогрес
            if total_processed % 10 == 0:
                self.stdout.write(f"Оброблено: {total_processed}/{len(bankruptcy_cases)}")
        
        self.stdout.write(
            self.style.SUCCESS(f"Завершено. Оброблено {total_processed} справ, знайдено {total_found} рішень")
        )

    def _get_court_decision_tables(self):
        """Отримання списку таблиць судових рішень"""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'court_decisions_%"
                ORDER BY table_name DESC
            """)
            
            return [row[0] for row in cursor.fetchall()]

    def _search_decisions_for_case(self, bankruptcy_case, tables):
        """Пошук судових рішень для конкретної справи банкрутства"""
        case_number = bankruptcy_case.case_number
        
        # Різні варіанти номера справи для пошуку
        search_patterns = [
            case_number,                    # Точний номер
            case_number.replace("/", "\\/"),  # З екрануванням слешу
            case_number.split("/")[0],      # Тільки перша частина
        ]
        
        all_decisions = []
        
        for table_name in tables:
            for pattern in search_patterns:
                decisions = self._search_in_table(table_name, pattern)
                if decisions:
                    for decision in decisions:
                        decision["bankruptcy_case_id"] = bankruptcy_case.id
                        decision["source_table"] = table_name
                        all_decisions.append(decision)
        
        # Видаляємо дублікати по doc_id
        unique_decisions = {}
        for decision in all_decisions:
            doc_id = decision["doc_id"]
            if doc_id not in unique_decisions:
                unique_decisions[doc_id] = decision
        
        return list(unique_decisions.values())

    def _search_in_table(self, table_name, case_pattern):
        """Пошук в конкретній таблиці"""
        with connection.cursor() as cursor:
            search_sql = f"""
                SELECT doc_id, court_code, judgment_code, cause_num, 
                       adjudication_date, judge, doc_url, status,
                       court_name, judgment_name, resolution_text
                FROM {table_name}
                WHERE cause_num ILIKE %s
                ORDER BY adjudication_date DESC
            """
            
            try:
                cursor.execute(search_sql, [f"%{case_pattern}%"])
                
                columns = [desc[0] for desc in cursor.description]
                results = []
                
                for row in cursor.fetchall():
                    result_dict = dict(zip(columns, row))
                    results.append(result_dict)
                
                return results
                
            except Exception as e:
                logger.error(f"Помилка пошуку в таблиці {table_name}: {e}")
                return []