from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.db import models
import logging
import time
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from bankruptcy.models import SystemProcessControl, BankruptcyCase, TrackedBankruptcyCase
from bankruptcy.services import BankruptcyCaseSearchService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Примусовий пошук судових рішень з зупинкою всіх інших процесів"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_control = None
        self.should_stop = False
        self.stats_lock = Lock()
        self.processed_count = 0
        self.found_decisions_total = 0
        self.errors_count = 0
        
        # Налаштування обробки сигналів зупинки
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обробка сигналів зупинки"""
        self.stdout.write(self.style.WARNING("Отримано сигнал зупинки. Зупиняємо процес..."))
        self.should_stop = True
    
    def process_single_case(self, case, service, total_cases):
        """Обробка однієї справи (для багатопоточності)"""
        try:
            # Створюємо або отримуємо відстежувану справу
            tracked_case, created = TrackedBankruptcyCase.objects.get_or_create(
                bankruptcy_case=case,
                defaults={
                    "status": "active",
                    "priority": 1,
                }
            )
            
            if created:
                self.stdout.write(f"➕ Додано до відстеження справу: {case.case_number}")
            
            # Виконуємо пошук судових рішень
            found_decisions = service.search_and_save_court_decisions(tracked_case)
            
            # Оновлюємо статистику (з блокуванням для thread-safety)
            with self.stats_lock:
                self.processed_count += 1
                self.found_decisions_total += found_decisions
                
                if found_decisions > 0:
                    self.stdout.write(
                        f"✅ Справа {case.case_number}: знайдено {found_decisions} рішень"
                    )
                
                # Оновлюємо прогрес кожні 10 справ
                if self.processed_count % 10 == 0:
                    progress_msg = f"Оброблено {self.processed_count}/{total_cases} справ, знайдено {self.found_decisions_total} рішень"
                    self.process_control.update_progress(self.processed_count, total_cases, progress_msg)
                    
                    percentage = (self.processed_count / total_cases) * 100
                    self.stdout.write(f"📈 Прогрес: {percentage:.1f}% ({self.processed_count}/{total_cases})")
            
            return found_decisions
            
        except Exception as e:
            with self.stats_lock:
                self.errors_count += 1
                self.processed_count += 1
            
            error_msg = f"❌ Помилка при обробці справи {case.case_number}: {str(e)}"
            self.stdout.write(self.style.ERROR(error_msg))
            logger.error(error_msg)
            return 0

    def add_arguments(self, parser):
        parser.add_argument(
            "--all-cases",
            action="store_true",
            help="Пошук для всіх справ банкрутства (не тільки відстежуваних)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Максимальна кількість справ для обробки",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help="Розмір батча для обробки (за замовчуванням: 50)",
        )
        parser.add_argument(
            "--year-from",
            type=int,
            help="Рік з якого починати пошук (за замовчуванням: всі роки)",
        )
        parser.add_argument(
            "--year-to",
            type=int,
            help="Рік до якого шукати включно (за замовчуванням: поточний рік)",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=8,
            help="Кількість паралельних потоків для пошуку (за замовчуванням: 8)",
        )
        parser.add_argument(
            "--only-without-decisions",
            action="store_true",
            help="Пошук тільки для справ без судових рішень",
        )

    def handle(self, *args, **options):
        all_cases = options["all_cases"]
        limit = options["limit"]
        batch_size = options["batch_size"]
        workers = options["workers"]
        year_from = options["year_from"]
        year_to = options["year_to"]
        only_without_decisions = options["only_without_decisions"]

        self.stdout.write("🚀 Запуск примусового пошуку судових рішень...")

        try:
            # Отримуємо або створюємо запис управління процесом
            self.process_control, created = SystemProcessControl.objects.get_or_create(
                process_type="court_search",
                defaults={
                    "status": "idle",
                    "is_forced": False,
                    "force_stop_others": True,
                }
            )
            
            if created:
                self.stdout.write("✅ Створено новий запис управління процесом пошуку")
            
            # Перевіряємо чи немає вже запущеного примусового процесу
            if SystemProcessControl.is_any_process_forced():
                existing_process = SystemProcessControl.get_forced_process()
                if existing_process and existing_process.pk != self.process_control.pk:
                    self.stdout.write(
                        self.style.ERROR(
                            f"❌ Вже запущено примусовий процес: {existing_process.get_process_type_display()}"
                        )
                    )
                    return
            
            # Запускаємо примусовий режим (це зупинить всі інші процеси)
            self.process_control.force_stop_others = True
            self.process_control.start_forced()
            
            self.stdout.write("⏸️ Всі інші процеси зупинено")
            self.stdout.write("🔍 Початок примусового пошуку судових рішень...")
            
            # Визначаємо набір справ для пошуку
            if all_cases:
                # Пошук для всіх справ банкрутства
                cases_queryset = BankruptcyCase.objects.all()
                self.stdout.write("📋 Режим: пошук для ВСІХ справ банкрутства")
            else:
                # Тільки для відстежуваних справ
                cases_queryset = BankruptcyCase.objects.filter(
                    id__in=TrackedBankruptcyCase.objects.values_list("bankruptcy_case_id", flat=True)
                )
                self.stdout.write("📋 Режим: пошук тільки для відстежуваних справ")
            
            # Фільтруємо за роками якщо вказано
            if year_from:
                cases_queryset = cases_queryset.filter(date__year__gte=year_from)
                self.stdout.write(f"📅 Фільтр: справи з {year_from} року")
            
            if year_to:
                cases_queryset = cases_queryset.filter(date__year__lte=year_to)
                self.stdout.write(f"📅 Фільтр: справи до {year_to} року включно")
            
            # Фільтруємо справи без судових рішень якщо вказано
            if only_without_decisions:
                # Знаходимо ID справ що НЕ мають судових рішень
                cases_with_decisions = TrackedBankruptcyCase.objects.annotate(
                    decision_count=models.Count("tracked_court_decisions")
                ).filter(decision_count__gt=0).values_list("bankruptcy_case_id", flat=True)
                
                cases_queryset = cases_queryset.exclude(id__in=cases_with_decisions)
                self.stdout.write("🔍 Фільтр: тільки справи БЕЗ судових рішень")
            
            # Сортуємо за датою (новіші спочатку)
            cases_queryset = cases_queryset.order_by("-date")
            
            # Застосовуємо ліміт якщо вказано
            if limit:
                cases_queryset = cases_queryset[:limit]
                self.stdout.write(f"🔢 Ліміт: максимум {limit} справ")
            
            total_cases = cases_queryset.count()
            self.stdout.write(f"📊 Загальна кількість справ для пошуку: {total_cases}")
            
            if total_cases == 0:
                self.stdout.write(self.style.WARNING("⚠️ Не знайдено справ для пошуку"))
                self.process_control.stop_forced()
                return
            
            # Ініціалізуємо прогрес
            self.process_control.update_progress(0, total_cases, "Початок пошуку...")
            
            # Ініціалізуємо сервіс пошуку
            service = BankruptcyCaseSearchService()
            
            processed_count = 0
            found_decisions_total = 0
            errors_count = 0
            
            # Обробляємо справи батчами
            batch_start = 0
            while batch_start < total_cases and not self.should_stop:
                batch_cases = cases_queryset[batch_start:batch_start + batch_size]
                
                self.stdout.write(f"🔄 Обробка батчу {batch_start + 1}-{min(batch_start + batch_size, total_cases)} з {total_cases}")
                
                for case in batch_cases:
                    if self.should_stop:
                        break
                    
                    try:
                        # Створюємо або отримуємо відстежувану справу
                        tracked_case, created = TrackedBankruptcyCase.objects.get_or_create(
                            bankruptcy_case=case,
                            defaults={
                                "status": "active",
                                "priority": 1,
                            }
                        )
                        
                        if created:
                            self.stdout.write(f"➕ Додано до відстеження справу: {case.case_number}")
                        
                        # Виконуємо пошук судових рішень
                        found_decisions = service.search_and_save_court_decisions(tracked_case)
                        found_decisions_total += found_decisions
                        
                        if found_decisions > 0:
                            self.stdout.write(
                                f"✅ Справа {case.case_number}: знайдено {found_decisions} рішень"
                            )
                        
                    except Exception as e:
                        errors_count += 1
                        error_msg = f"❌ Помилка при обробці справи {case.case_number}: {str(e)}"
                        self.stdout.write(self.style.ERROR(error_msg))
                        logger.error(error_msg)
                    
                    processed_count += 1
                    
                    # Оновлюємо прогрес
                    progress_msg = f"Оброблено {processed_count}/{total_cases} справ, знайдено {found_decisions_total} рішень"
                    self.process_control.update_progress(processed_count, total_cases, progress_msg)
                    
                    # Показуємо прогрес кожні 10 справ
                    if processed_count % 10 == 0:
                        percentage = (processed_count / total_cases) * 100
                        self.stdout.write(f"📈 Прогрес: {percentage:.1f}% ({processed_count}/{total_cases})")
                
                batch_start += batch_size
                
                # Невелика пауза між батчами
                if not self.should_stop:
                    time.sleep(1)
            
            # Завершуємо процес
            if self.should_stop:
                final_msg = f"⏹️ Процес зупинено користувачем. Оброблено {processed_count}/{total_cases} справ"
            else:
                final_msg = f"✅ Пошук завершено успішно. Оброблено {processed_count}/{total_cases} справ"
            
            self.process_control.update_progress(processed_count, total_cases, final_msg)
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n🎉 Примусовий пошук судових рішень завершено:\n"
                    f"   📊 Оброблено справ: {processed_count}\n"
                    f"   🔍 Знайдено рішень: {found_decisions_total}\n"
                    f"   ❌ Помилок: {errors_count}\n"
                    f"   ⏱️ Час виконання: {timezone.now() - self.process_control.started_at}"
                )
            )

        except Exception as e:
            error_msg = f"Критична помилка при примусовому пошуку: {str(e)}"
            self.stdout.write(self.style.ERROR(f"❌ {error_msg}"))
            logger.error(error_msg)
            
            if self.process_control:
                self.process_control.last_message = f"Помилка: {str(e)}"
                self.process_control.status = "error"
                self.process_control.save()

        finally:
            # Завжди зупиняємо примусовий режим
            if self.process_control:
                self.process_control.stop_forced()
                self.stdout.write("🔄 Відновлено штатний режим роботи системи")
                
            # Автоматично відновлюємо фоновий пошук
            self._restart_background_services()
    
    def _restart_background_services(self):
        """Відновлює фонові сервіси після завершення примусового пошуку"""
        try:
            import subprocess
            self.stdout.write("🚀 Відновлюємо автоматичний пошук судових рішень...")
            
            # Перезапускаємо службу bankruptcy-monitor
            result = subprocess.run([
                "sudo", "-S", "systemctl", "start", "bankruptcy-monitor.service"
            ], input="130184srv\n", text=True, capture_output=True)
            
            if result.returncode == 0:
                self.stdout.write("✅ Автоматичний пошук судових рішень відновлено")
            else:
                self.stdout.write("⚠️ Не вдалося автоматично відновити службу bankruptcy-monitor")
                self.stdout.write("   Запустіть вручну: sudo systemctl start bankruptcy-monitor.service")
                
        except Exception as e:
            self.stdout.write(f"⚠️ Помилка при відновленні фонових сервісів: {e}")
            self.stdout.write("   Запустіть вручну: sudo systemctl start bankruptcy-monitor.service")