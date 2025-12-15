import os
import sqlite3
import time
import re
import logging
from datetime import datetime, timedelta
from django.conf import settings
from django.utils import timezone
from django.db import transaction
from django.db.models import Q
from bankruptcy.models import TrackedBankruptcyCase, TrackedCourtDecision
import concurrent.futures
import threading


class FastCourtSearch:
    """
    Високопродуктивний сервіс пошуку судових рішень на основі алгоритмів SR_AI
    """
    
    def __init__(self):
        self.db_dir = getattr(settings, "COURT_DECISIONS_DB_DIR", "/home/ruslan/PYTHON/analiz_klientiv/data/search_databases")
        self.batch_size = getattr(settings, "SEARCH_BATCH_SIZE", 1000)
        self.max_workers = getattr(settings, "SEARCH_MAX_WORKERS", 3)
        
        # Створюємо директорію для баз даних
        os.makedirs(self.db_dir, exist_ok=True)
        
        # Налаштування логування
        self.setup_logging()
        
        # Кеш з"єднань до баз даних
        self.db_connections = {}
        self.connection_lock = threading.Lock()
        
        # Доступні роки баз даних (автоматично визначаємо)
        self.available_years = self.get_available_database_years()
        
    def setup_logging(self):
        """Налаштовує логування для сервісу"""
        log_dir = os.path.join(settings.BASE_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"court_search_{datetime.now().strftime("%Y%m%d")}.log")
        
        self.logger = logging.getLogger("fast_court_search")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file, encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def get_available_database_years(self):
        """
        Автоматично визначає доступні роки баз даних
        """
        years = []
        
        # Шукаємо таблиці court_decisions_YYYY в основній базі PostgreSQL
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name LIKE 'court_decisions_%'
                AND table_schema = 'public'
            """)
            
            for row in cursor.fetchall():
                table_name = row[0]
                year_match = re.search(r"court_decisions_(\d{4})", table_name)
                if year_match:
                    year = int(year_match.group(1))
                    if 2014 <= year <= 2030:  # Розумні межі років (включаючи 2014)
                        years.append(year)
        
        # Також шукаємо SQLite бази в директорії (якщо є)
        if os.path.exists(self.db_dir):
            for filename in os.listdir(self.db_dir):
                if filename.startswith("documents_") and filename.endswith(".db"):
                    year_match = re.search(r"documents_(\d{2,4})\.db", filename)
                    if year_match:
                        year_str = year_match.group(1)
                        if len(year_str) == 2:
                            year = 2000 + int(year_str)
                        else:
                            year = int(year_str)
                        
                        if 2015 <= year <= 2030 and year not in years:
                            years.append(year)
        
        return sorted(years, reverse=True)  # Від нових до старих
    
    def get_sqlite_connection(self, year):
        """
        Отримує з"єднання до SQLite бази для конкретного року
        """
        db_path = os.path.join(self.db_dir, f"documents_{year}.db")
        
        if not os.path.exists(db_path):
            # Спробуємо формат з двома цифрами року
            year_short = year % 100
            db_path = os.path.join(self.db_dir, f"documents_{year_short:02d}.db")
            
            if not os.path.exists(db_path):
                return None
        
        with self.connection_lock:
            cache_key = f"sqlite_{year}"
            if cache_key not in self.db_connections:
                try:
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                    self.db_connections[cache_key] = conn
                except Exception as e:
                    self.logger.error(f"Помилка підключення до SQLite бази {db_path}: {e}")
                    return None
            
            return self.db_connections[cache_key]
    
    def search_in_postgres_table(self, case_number, year):
        """
        Точний пошук рішень в PostgreSQL таблиці для конкретного року (адаптація SR_AI)
        Використовує hash індекси для швидкого точного пошуку
        """
        from django.db import connection
        
        table_name = f"court_decisions_{year}"
        
        # Генеруємо точні варіанти номера справи
        search_variants = self.generate_exact_case_variants(case_number)
        
        results = []
        
        try:
            with connection.cursor() as cursor:
                # Перевіряємо існування таблиці
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = %s AND table_schema = "public"
                    )
                """, [table_name])
                
                if not cursor.fetchone()[0]:
                    return results
                
                # ТОЧНИЙ пошук для кожного варіанту (використовує hash індекс)
                for variant in search_variants:
                    cursor.execute(f"""
                        SELECT id, cause_num, court_code, judgment_code, 
                               judge, doc_url, doc_id, adjudication_date
                        FROM {table_name}
                        WHERE cause_num = %s
                        ORDER BY adjudication_date DESC
                    """, [variant])
                    
                    for row in cursor.fetchall():
                        result = {
                            "source_year": year,
                            "source_table": table_name,
                            "cause_num": row[1],
                            "court_code": row[2],
                            "judgment_code": row[3],
                            "judge": row[4],
                            "doc_url": row[5],
                            "doc_id": row[6],
                            "date_decision": row[7]
                        }
                        # Уникаємо дублікатів
                        if not any(r["doc_id"] == result["doc_id"] for r in results):
                            results.append(result)
        
        except Exception as e:
            self.logger.error(f"Помилка точного пошуку в PostgreSQL таблиці {table_name}: {e}")
        
        return results
    
    def search_in_sqlite_db(self, case_number, year):
        """
        Точний пошук рішень в SQLite базі для конкретного року (адаптація SR_AI)
        Використовує індексовані точні запити
        """
        conn = self.get_sqlite_connection(year)
        if not conn:
            return []
        
        # Генеруємо точні варіанти номера справи
        search_variants = self.generate_exact_case_variants(case_number)
        
        results = []
        
        try:
            cursor = conn.cursor()
            
            # Перевіряємо існування таблиці
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='documents'")
            if not cursor.fetchone():
                return results
            
            # ТОЧНИЙ пошук для кожного варіанту (використовує B-tree індекс)
            for variant in search_variants:
                cursor.execute("""
                    SELECT cause_num, court_code, judgment_code, 
                           judge, doc_url, doc_id, date_decision
                    FROM documents
                    WHERE cause_num = ?
                    ORDER BY date_decision DESC
                """, [variant])
                
                for row in cursor.fetchall():
                    result = {
                        "source_year": year,
                        "source_db": f"documents_{year}.db",
                        "cause_num": row[0],
                        "court_code": row[1],
                        "judgment_code": row[2],
                        "judge": row[3],
                        "doc_url": row[4],
                        "doc_id": row[5],
                        "date_decision": row[6]
                    }
                    # Уникаємо дублікатів
                    if not any(r["doc_id"] == result["doc_id"] for r in results):
                        results.append(result)
        
        except Exception as e:
            self.logger.error(f"Помилка точного пошуку в SQLite базі для року {year}: {e}")
        
        return results
    
    def generate_exact_case_variants(self, case_number):
        """
        Генерує точні варіанти номера справи для точного пошуку (адаптація SR_AI)
        Замість пошуку по шаблону - генеруємо точні варіанти для = запитів
        """
        if not case_number or not case_number.strip():
            return []
            
        variants = []
        clean_case = case_number.strip()
        variants.append(clean_case)
        
        # Розбираємо номер справи на частини: 756/16936/23
        parts = clean_case.split("/")
        
        if len(parts) >= 3:
            court_code = parts[0]
            case_num = parts[1] 
            year_part = parts[2]
            
            # Додаємо варіант з повним роком
            if len(year_part) == 2 and year_part.isdigit():
                year_int = int(year_part)
                # Логіка визначення століття (як у SR_AI)
                if year_int <= 30:
                    full_year = f"20{year_part}"
                else:
                    full_year = f"19{year_part}"
                    
                full_variant = f"{court_code}/{case_num}/{full_year}"
                if full_variant != clean_case:
                    variants.append(full_variant)
            
            # Додаємо варіант з скороченим роком
            elif len(year_part) == 4 and year_part.isdigit():
                short_year = year_part[-2:]
                short_variant = f"{court_code}/{case_num}/{short_year}"
                if short_variant != clean_case:
                    variants.append(short_variant)
        
        # Нормалізація: видаляємо зайві пробіли та символи
        normalized_variants = []
        for variant in variants:
            # Видаляємо зайві пробіли
            normalized = re.sub(r"\s+", "", variant)
            # Залишаємо тільки цифри та слеші
            normalized = re.sub(r"[^\d/]", "", normalized)
            if normalized and normalized not in normalized_variants:
                normalized_variants.append(normalized)
        
        return normalized_variants
    
    def generate_case_number_variants(self, case_number):
        """
        DEPRECATED: Старий метод нечіткого пошуку
        Використовуй generate_exact_case_variants для точного пошуку
        """
        return self.generate_exact_case_variants(case_number)
    
    def search_single_case_exact(self, case):
        """
        Швидкий точний пошук рішень для однієї справи (адаптація SR_AI з паралелізмом)
        """
        if not case.bankruptcy_case.case_number:
            return []
        
        all_results = []
        case_number = case.bankruptcy_case.case_number
        
        # Паралельний пошук в усіх роках (як у SR_AI)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(self.available_years))) as executor:
            # Створюємо завдання для кожного року
            future_to_year = {}
            
            for year in self.available_years:
                # PostgreSQL пошук
                postgres_future = executor.submit(self.search_in_postgres_table, case_number, year)
                future_to_year[postgres_future] = f"postgres_{year}"
                
                # SQLite пошук (якщо потрібно)
                sqlite_future = executor.submit(self.search_in_sqlite_db, case_number, year)
                future_to_year[sqlite_future] = f"sqlite_{year}"
            
            # Збираємо результати по мірі завершення
            for future in concurrent.futures.as_completed(future_to_year):
                source = future_to_year[future]
                try:
                    results = future.result()
                    if results:
                        all_results.extend(results)
                        self.logger.info(f"Знайдено {len(results)} рішень в {source} для справи {case_number}")
                except Exception as e:
                    self.logger.error(f"Помилка пошуку в {source}: {e}")
        
        created_decisions = self._save_found_decisions(case, all_results)
        
        # ВАЖЛИВО: Закриваємо з"єднання після використання
        self.close_connections()
        
        # Повертаємо знайдені рішення (дані) а не створені об"єкти для тестування
        return all_results
    
    def search_single_case(self, case):
        """
        LEGACY: Старий метод пошуку (залишено для сумісності)
        Рекомендовано використовувати search_single_case_exact
        """
        return self.search_single_case_exact(case)
    
    def _save_found_decisions(self, case, all_results):
        """
        Зберігає знайдені рішення в базу даних
        """
        created_decisions = []
        
        for result in all_results:
            try:
                # Перевіряємо чи вже існує таке рішення
                existing_decision = TrackedCourtDecision.objects.filter(
                    tracked_case=case,
                    doc_id=result.get("doc_id"),
                    cause_num=result.get("cause_num")
                ).first()
                
                if not existing_decision:
                    # Створюємо нове рішення
                    decision = TrackedCourtDecision.objects.create(
                        tracked_case=case,
                        cause_num=result.get("cause_num", ""),
                        court_code=result.get("court_code", ""),
                        judgment_code=result.get("judgment_code", ""),
                        judge=result.get("judge", ""),
                        doc_url=result.get("doc_url", ""),
                        doc_id=result.get("doc_id", ""),
                        adjudication_date=result.get("date_decision", result.get("adjudication_date", "")),
                        database_source=f"Точний пошук: {result.get("source_table", result.get("source_db", "невідомо"))}"
                    )
                    created_decisions.append(decision)
            
            except Exception as e:
                self.logger.error(f"Помилка збереження рішення для справи {case.id}: {e}")
        
        if created_decisions:
            # Оновлюємо час останнього пошуку для справи
            case.search_decisions_completed_at = timezone.now()
            case.save(update_fields=["search_decisions_completed_at"])
            
            self.logger.info(f"Збережено {len(created_decisions)} нових рішень для справи {case.bankruptcy_case.case_number}")
        
        return created_decisions
    
    def search_cases_batch(self, limit=None):
        """
        Виконує пошук рішень для справ без рішень або з застарілим пошуком (батчами)
        """
        if limit is None:
            limit = self.batch_size
        
        # Отримуємо справи для пошуку
        cases_to_search = TrackedBankruptcyCase.objects.filter(
            bankruptcy_case__case_number__isnull=False
        ).exclude(
            bankruptcy_case__case_number__exact=""
        ).exclude(
            bankruptcy_case__case_number__exact="nan"
        ).filter(
            Q(search_decisions_completed_at__isnull=True) |
            Q(search_decisions_completed_at__lt=timezone.now() - timedelta(days=7))
        ).order_by("-priority", "bankruptcy_case__date", "created_at")[:limit]
        
        if not cases_to_search:
            return {
                "success": True,
                "processed": 0,
                "found_decisions": 0,
                "message": "Немає справ для пошуку"
            }
        
        print(f"Початок пошуку рішень для {len(cases_to_search)} справ...")
        
        total_found = 0
        processed_count = 0
        
        # Обробляємо справи батчами з використанням потоків
        batch_size = 5  # Менший розмір батчу для ThreadPool
        batches = [cases_to_search[i:i+batch_size] for i in range(0, len(cases_to_search), batch_size)]
        
        for batch_index, batch in enumerate(batches):
            print(f"Обробка батчу {batch_index+1}/{len(batches)}...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_case = {
                    executor.submit(self.search_single_case, case): case
                    for case in batch
                }
                
                for future in concurrent.futures.as_completed(future_to_case):
                    case = future_to_case[future]
                    try:
                        found_decisions = future.result()
                        total_found += len(found_decisions)
                        processed_count += 1
                        
                        if found_decisions:
                            print(f"Справа {case.bankruptcy_case.case_number}: знайдено {len(found_decisions)} рішень")
                    
                    except Exception as e:
                        self.logger.error(f"Помилка пошуку для справи {case.id}: {e}")
                        processed_count += 1
            
            print(f"Завершено батч {batch_index+1}/{len(batches)}. "
                  f"Оброблено: {processed_count}/{len(cases_to_search)}")
        
        print(f"Пошук завершено. Оброблено справ: {processed_count}, знайдено рішень: {total_found}")
        
        return {
            "success": True,
            "processed": processed_count,
            "found_decisions": total_found
        }
    
    def search_continuous(self):
        """
        Безперервний пошук судових рішень
        """
        print("Запуск безперервного пошуку судових рішень...")
        
        while True:
            try:
                # Перевіряємо чи є справи для пошуку
                pending_count = TrackedBankruptcyCase.objects.filter(
                    bankruptcy_case__case_number__isnull=False
                ).exclude(
                    bankruptcy_case__case_number__exact=""
                ).exclude(
                    bankruptcy_case__case_number__exact="nan"
                ).filter(
                    Q(search_decisions_completed_at__isnull=True) |
                    Q(search_decisions_completed_at__lt=timezone.now() - timedelta(days=7))
                ).count()
                
                if pending_count == 0:
                    print("Всі справи оброблено. Очікування нових справ...")
                    time.sleep(60)  # Очікуємо 1 хвилину
                    continue
                
                print(f"Знайдено {pending_count} справ для пошуку")
                
                # Обробляємо батч
                result = self.search_cases_batch(self.batch_size)
                
                if result["processed"] == 0:
                    print("Немає справ для обробки. Очікування...")
                    time.sleep(30)
                else:
                    print(f"Оброблено {result["processed"]} справ, "
                          f"знайдено {result["found_decisions"]} рішень")
                
                # Невелика пауза між батчами
                time.sleep(5)
                
            except KeyboardInterrupt:
                print("Зупинка безперервного пошуку за запитом користувача")
                break
            except Exception as e:
                self.logger.error(f"Помилка в безперервному пошуку: {e}")
                print(f"Помилка в безперервному пошуку: {e}")
                time.sleep(10)  # Пауза перед повторною спробою
    
    def close_connections(self):
        """
        Закриває всі відкриті з"єднання до баз даних
        """
        with self.connection_lock:
            for connection in self.db_connections.values():
                try:
                    connection.close()
                except:
                    pass
            self.db_connections.clear()
        
        # Закриваємо Django з"єднання
        from django.db import connections
        for conn in connections.all():
            conn.close()


# Утилітні функції для використання в командах та views
def search_court_decisions_fast(limit=None):
    """
    Швидкий пошук судових рішень
    """
    searcher = FastCourtSearch()
    try:
        return searcher.search_cases_batch(limit)
    finally:
        searcher.close_connections()


def start_continuous_search():
    """
    Запускає безперервний пошук судових рішень
    """
    searcher = FastCourtSearch()
    try:
        searcher.search_continuous()
    finally:
        searcher.close_connections()


def get_search_statistics():
    """
    Отримує статистику пошуку судових рішень
    Використовує менеджер з"єднань для безпечної роботи
    """
    try:
        # Використовуємо менеджер з"єднань для безпечних запитів
        from bankruptcy.utils.connection_manager import safe_db_connection
        from django.db.models import Count
        
        with safe_db_connection() as connection:
            total_cases = TrackedBankruptcyCase.objects.count()
            
            cases_with_decisions = TrackedBankruptcyCase.objects.annotate(
                decision_count=Count("tracked_court_decisions")
            ).filter(decision_count__gt=0).count()
            
            pending_cases = TrackedBankruptcyCase.objects.filter(
                bankruptcy_case__case_number__isnull=False
            ).exclude(
                bankruptcy_case__case_number__exact=""
            ).exclude(
                bankruptcy_case__case_number__exact="nan"
            ).filter(
                Q(search_decisions_completed_at__isnull=True) |
                Q(search_decisions_completed_at__lt=timezone.now() - timedelta(days=7))
            ).count()
            
            total_decisions = TrackedCourtDecision.objects.count()
            
            recent_decisions = TrackedCourtDecision.objects.filter(
                found_at__gte=timezone.now() - timedelta(hours=1)
            ).count()
    except Exception as e:
        # Якщо помилка з"єднання - повертаємо базову статистику
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Помилка отримання статистики пошуку: {e}")
        return {
            "total_cases": 0,
            "cases_with_decisions": 0,
            "pending_cases": 0,
            "search_percentage": 0,
            "total_decisions": 0,
            "recent_decisions": 0,
            "error": str(e)
        }
    
    return {
        "total_cases": total_cases,
        "cases_with_decisions": cases_with_decisions,
        "pending_cases": pending_cases,
        "total_decisions": total_decisions,
        "recent_decisions": recent_decisions,
        "search_percentage": (cases_with_decisions / total_cases * 100) if total_cases > 0 else 0
    }