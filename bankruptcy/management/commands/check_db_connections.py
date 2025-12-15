"""
Команда для перевірки статусу з"єднань до PostgreSQL
"""
from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.utils.connection_manager import get_connection_status
import psycopg2

class Command(BaseCommand):
    help = "Перевіряє статус з\"єднань до PostgreSQL та показує поточне використання"

    def add_arguments(self, parser):
        parser.add_argument(
            "--detailed",
            action="store_true",
            help="Показує детальну інформацію про з\"єднання PostgreSQL",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=== СТАТУС З"ЄДНАНЬ ДО POSTGRESQL ===\n"))

        # Статус нашого менеджера з"єднань
        try:
            status = get_connection_status()
            self.stdout.write(f"📊 Менеджер з"єднань Django:")
            self.stdout.write(f"   - Активних з"єднань: {status["active_connections"]}/{status["max_connections"]}")
            self.stdout.write(f"   - Активних потоків: {status["active_threads"]}/{status["max_threads"]}")
            self.stdout.write(f"   - Доступних з"єднань: {status["available_connections"]}")
            self.stdout.write(f"   - Доступних потоків: {status["available_threads"]}")
            
            # Кольорове попередження
            if status["active_connections"] > status["max_connections"] * 0.8:
                self.stdout.write(self.style.WARNING(f"⚠️  ПОПЕРЕДЖЕННЯ: Високе використання з"єднань ({status["active_connections"]}/{status["max_connections"]})"))
            else:
                self.stdout.write(self.style.SUCCESS(f"✅ Нормальне використання з"єднань"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Помилка отримання статусу менеджера з"єднань: {e}"))

        # Статус PostgreSQL сервера
        if options["detailed"]:
            try:
                with connection.cursor() as cursor:
                    # Загальна кількість з"єднань
                    cursor.execute("SELECT count(*) FROM pg_stat_activity;")
                    total_connections = cursor.fetchone()[0]
                    
                    # З"єднання від нашої програми
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE "%django%" OR datname = %s;", ["analiz_klientiv"])
                    django_connections = cursor.fetchone()[0]
                    
                    # Максимальна кількість з"єднань PostgreSQL
                    cursor.execute("SELECT setting FROM pg_settings WHERE name = "max_connections";")
                    result = cursor.fetchone()
                    max_pg_connections = int(result[0]) if result else 100
                    
                    # Активні запити
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE state = "active";")
                    active_queries = cursor.fetchone()[0]
                    
                    # Очікуючі запити (заблоковані)
                    cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL;")
                    waiting_queries = cursor.fetchone()[0]
                    
                    self.stdout.write(f"\n🗄️  PostgreSQL сервер:")
                    self.stdout.write(f"   - Всього з"єднань: {total_connections}/{max_pg_connections}")
                    self.stdout.write(f"   - З"єднань від Django: {django_connections}")
                    self.stdout.write(f"   - Активних запитів: {active_queries}")
                    self.stdout.write(f"   - Очікуючих запитів: {waiting_queries}")
                    
                    usage_percent = (total_connections / max_pg_connections) * 100
                    if usage_percent > 90:
                        self.stdout.write(self.style.ERROR(f"🚨 КРИТИЧНО: Використання з"єднань {usage_percent:.1f}% - близько до ліміту!"))
                    elif usage_percent > 70:
                        self.stdout.write(self.style.WARNING(f"⚠️  ПОПЕРЕДЖЕННЯ: Використання з"єднань {usage_percent:.1f}%"))
                    else:
                        self.stdout.write(self.style.SUCCESS(f"✅ Нормальне використання з"єднань: {usage_percent:.1f}%"))
                    
                    # Топ активних запитів
                    if active_queries > 0:
                        cursor.execute("""
                            SELECT query, state, query_start, application_name 
                            FROM pg_stat_activity 
                            WHERE state = "active" AND query NOT LIKE "%pg_stat_activity%" 
                            ORDER BY query_start 
                            LIMIT 5;
                        """)
                        
                        active_queries_info = cursor.fetchall()
                        if active_queries_info:
                            self.stdout.write(f"\n🔄 Топ активних запитів:")
                            for i, (query, state, query_start, app_name) in enumerate(active_queries_info, 1):
                                query_short = query[:100] + "..." if len(query) > 100 else query
                                self.stdout.write(f"   {i}. [{app_name}] {query_short}")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Помилка отримання детальної інформації PostgreSQL: {e}"))

        # Рекомендації
        self.stdout.write(f"\n💡 Рекомендації:")
        self.stdout.write(f"   - Максимум з"єднань в settings.py: MAX_TOTAL_DB_CONNECTIONS")
        self.stdout.write(f"   - Максимум потоків в settings.py: MAX_CONCURRENT_THREADS")
        self.stdout.write(f"   - Використовуйте менеджер з"єднань для безпечної роботи")
        self.stdout.write(f"   - Регулярно запускайте цю команду для моніторингу")
        
        self.stdout.write(self.style.SUCCESS(f"\n✅ Перевірку завершено"))