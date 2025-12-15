from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from bankruptcy.utils.index_optimizer import index_optimizer


class Command(BaseCommand):
    help = "Показує статистику автоматичної оптимізації індексів"

    def add_arguments(self, parser):
        parser.add_argument(
            "--cleanup",
            action="store_true",
            help="Очистити старі записи з лог-файлу (старіше 30 днів)",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=30,
            help="Кількість днів для збереження логів при очищенні (за замовчуванням: 30)",
        )

    def handle(self, *args, **options):
        cleanup = options["cleanup"]
        days_to_keep = options["days"]

        self.stdout.write("📊 СТАТИСТИКА АВТОМАТИЧНОЇ ОПТИМІЗАЦІЇ ІНДЕКСІВ")
        self.stdout.write("=" * 60)

        # Отримуємо статистику
        stats = index_optimizer.get_optimization_statistics()

        if stats["total_optimizations"] == 0:
            self.stdout.write("ℹ️ Оптимізації ще не виконувались")
            return

        # Основна статистика
        self.stdout.write(f"📈 Всього оптимізацій: {stats["total_optimizations"]}")
        self.stdout.write(f"✅ Успішних: {stats["successful_optimizations"]}")
        self.stdout.write(f"❌ Невдалих: {stats["failed_optimizations"]}")
        
        if stats["total_optimizations"] > 0:
            success_rate = (stats["successful_optimizations"] / stats["total_optimizations"]) * 100
            self.stdout.write(f"🎯 Відсоток успішності: {success_rate:.1f}%")

        # Остання оптимізація
        if stats["last_optimization"]:
            last_opt_time = timezone.localtime(stats["last_optimization"])
            time_ago = timezone.now() - stats["last_optimization"]
            
            self.stdout.write(f"🕒 Остання оптимізація: {last_opt_time.strftime("%Y-%m-%d %H:%M:%S")}")
            
            if time_ago < timedelta(hours=1):
                time_desc = f"{int(time_ago.total_seconds() / 60)} хвилин тому"
            elif time_ago < timedelta(days=1):
                time_desc = f"{int(time_ago.total_seconds() / 3600)} годин тому"
            else:
                time_desc = f"{time_ago.days} днів тому"
            
            self.stdout.write(f"⏰ Це було: {time_desc}")

        # Таблиці, що оптимізувались
        if stats["tables_optimized"]:
            self.stdout.write(f"\n📋 Оптимізовані таблиці ({len(stats["tables_optimized"])}):")
            for table in sorted(stats["tables_optimized"]):
                self.stdout.write(f"  • {table}")

        # Рекомендації
        self.stdout.write("\n🎯 РЕКОМЕНДАЦІЇ:")
        
        if stats["failed_optimizations"] > stats["successful_optimizations"]:
            self.stdout.write("⚠️ Велика кількість невдалих оптимізацій. Перевірте логи.")
        
        if stats["last_optimization"]:
            hours_since_last = (timezone.now() - stats["last_optimization"]).total_seconds() / 3600
            
            if hours_since_last > 168:  # 7 днів
                self.stdout.write("🔧 Давно не було оптимізації. Можливо, потрібна ручна оптимізація.")
            elif hours_since_last > 48:  # 2 дні
                self.stdout.write("ℹ️ Останню оптимізацію проводили давно, але це може бути нормально.")
            else:
                self.stdout.write("✅ Оптимізація виконується регулярно.")

        # Очищення логів
        if cleanup:
            self.stdout.write(f"\n🧹 Очищення логів старших за {days_to_keep} днів...")
            index_optimizer.cleanup_old_logs(days_to_keep)
            self.stdout.write("✅ Очищення завершено")

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("💡 Для ручної оптимізації: python manage.py optimize_court_indexes")
        self.stdout.write("📝 Логи зберігаються в: logs/index_optimization.log")