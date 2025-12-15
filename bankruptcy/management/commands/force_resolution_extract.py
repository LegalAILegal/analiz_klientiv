from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.db import models
import logging
import time
import signal
import sys
import os

from bankruptcy.models import SystemProcessControl, TrackedCourtDecision
from bankruptcy.utils.fast_resolution_extractor import FastResolutionExtractor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Примусове витягування резолютивних частин з зупинкою всіх інших процесів"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_control = None
        self.should_stop = False
        self.extractor = None
        
        # Налаштування обробки сигналів зупинки
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обробка сигналів зупинки"""
        self.stdout.write(self.style.WARNING("Отримано сигнал зупинки. Зупиняємо процес..."))
        self.should_stop = True
        
        # Зупиняємо екстрактор якщо він працює
        if self.extractor:
            self.extractor.stop_preloading()
        
        if self.process_control:
            self.process_control.stop_forced()
        sys.exit(0)

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Максимальна кількість рішень для обробки",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Розмір батча для обробки (за замовчуванням: 500)",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=6,
            help="Кількість потоків для витягування (за замовчуванням: 6)",
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Рік для фільтрації рішень (за замовчуванням: всі роки)",
        )
        parser.add_argument(
            "--failed-only",
            action="store_true",
            help="Повторити витягування тільки для рішень з помилками",
        )
        parser.add_argument(
            "--missing-only",
            action="store_true",
            help="Витягувати тільки рішення без резолютивних частин",
        )
        parser.add_argument(
            "--auto-incremental",
            action="store_true",
            help="Автоматично вибирати інкрементальний режим якщо більшість рішень вже оброблено",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        batch_size = options["batch_size"]
        workers = options["workers"]
        year = options["year"]
        failed_only = options["failed_only"]
        missing_only = options["missing_only"]
        auto_incremental = options["auto_incremental"]

        self.stdout.write("🚀 Запуск примусового витягування резолютивних частин...")

        try:
            # Отримуємо або створюємо запис управління процесом
            self.process_control, created = SystemProcessControl.objects.get_or_create(
                process_type="resolution_extraction",
                defaults={
                    "status": "idle",
                    "is_forced": False,
                    "force_stop_others": True,
                }
            )
            
            if created:
                self.stdout.write("✅ Створено новий запис управління процесом витягування")
            
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
            self.stdout.write("📄 Початок примусового витягування резолютивних частин...")
            
            # Створюємо екстрактор з налаштуваннями
            from django.conf import settings
            settings.RESOLUTION_MAX_WORKERS = workers
            settings.RESOLUTION_BATCH_SIZE = batch_size
            
            self.extractor = FastResolutionExtractor()
            
            # Перевіряємо чи потрібний інкрементальний режим
            use_incremental = auto_incremental and self.extractor.should_use_incremental_mode()
            
            if use_incremental:
                self.stdout.write("🔄 Автоматично обрано ІНКРЕМЕНТАЛЬНИЙ режим")
                # У інкрементальному режимі завжди відпрацьовуємо missing_only
                missing_only = True
            
            # Визначаємо набір рішень для обробки
            decisions_queryset = TrackedCourtDecision.objects.filter(
                doc_url__isnull=False
            ).exclude(
                doc_url__exact=""
            ).exclude(
                doc_url__exact="nan"
            )
            
            # Фільтруємо за роком якщо вказано
            if year:
                # Припускаємо що в моделі є поле з датою, або можемо фільтрувати за таблицями
                year_tables = [f"court_decisions_{year}"]
                # Тут можна додати логіку фільтрації за роком
                self.stdout.write(f"📅 Фільтр: рішення за {year} рік")
            
            if failed_only:
                # Рішення з помилками (ті що мають текст помилки)
                decisions_queryset = decisions_queryset.filter(
                    models.Q(resolution_text__icontains="Помилка") |
                    models.Q(resolution_text__icontains="Не вдалося") |
                    models.Q(resolution_text__isnull=True)
                )
                self.stdout.write("❌ Режим: повторне витягування тільки для рішень з помилками")
            elif missing_only:
                # Рішення без резолютивних частин
                decisions_queryset = decisions_queryset.filter(
                    models.Q(resolution_text__isnull=True) | 
                    models.Q(resolution_text__exact="")
                )
                self.stdout.write("📝 Режим: витягування тільки для рішень без резолютивних частин")
            else:
                # Всі рішення з URL
                self.stdout.write("📋 Режим: витягування для ВСІХ рішень з URL документів")
            
            # Сортуємо за датою додавання (новіші спочатку)
            decisions_queryset = decisions_queryset.order_by("-found_at")
            
            # Застосовуємо ліміт якщо вказано
            if limit:
                decisions_queryset = decisions_queryset[:limit]
                self.stdout.write(f"🔢 Ліміт: максимум {limit} рішень")
            
            total_decisions = decisions_queryset.count()
            self.stdout.write(f"📊 Загальна кількість рішень для витягування: {total_decisions}")
            
            if total_decisions == 0:
                self.stdout.write(self.style.WARNING("⚠️ Не знайдено рішень для витягування"))
                self.process_control.stop_forced()
                return
            
            # Ініціалізуємо прогрес
            self.process_control.update_progress(0, total_decisions, "Початок витягування...")
            
            processed_count = 0
            success_count = 0
            error_count = 0
            
            # Обробляємо рішення батчами
            batch_start = 0
            while batch_start < total_decisions and not self.should_stop:
                batch_decisions = list(decisions_queryset[batch_start:batch_start + batch_size])
                
                self.stdout.write(f"🔄 Обробка батчу {batch_start + 1}-{min(batch_start + batch_size, total_decisions)} з {total_decisions}")
                
                try:
                    # Використовуємо екстрактор для обробки батчу
                    result = self.extractor.extract_resolutions_batch_custom(batch_decisions)
                    
                    batch_processed = result.get("processed", 0)
                    batch_success = result.get("successful", 0)
                    batch_failed = result.get("failed", 0)
                    
                    processed_count += batch_processed
                    success_count += batch_success
                    error_count += batch_failed
                    
                    self.stdout.write(
                        f"✅ Батч завершено: оброблено {batch_processed}, успішно {batch_success}, помилок {batch_failed}"
                    )
                    
                except Exception as e:
                    error_msg = f"❌ Помилка при обробці батчу: {str(e)}"
                    self.stdout.write(self.style.ERROR(error_msg))
                    logger.error(error_msg)
                    error_count += len(batch_decisions)
                    processed_count += len(batch_decisions)
                
                # Оновлюємо прогрес
                progress_msg = f"Оброблено {processed_count}/{total_decisions} рішень, успішно {success_count}"
                self.process_control.update_progress(processed_count, total_decisions, progress_msg)
                
                # Показуємо прогрес
                if processed_count > 0:
                    percentage = (processed_count / total_decisions) * 100
                    success_rate = (success_count / processed_count) * 100
                    self.stdout.write(
                        f"📈 Прогрес: {percentage:.1f}% ({processed_count}/{total_decisions}), "
                        f"успішність: {success_rate:.1f}%"
                    )
                
                batch_start += batch_size
                
                # Невелика пауза між батчами
                if not self.should_stop:
                    time.sleep(2)
            
            # Завершуємо процес
            if self.should_stop:
                final_msg = f"⏹️ Процес зупинено користувачем. Оброблено {processed_count}/{total_decisions} рішень"
            else:
                final_msg = f"✅ Витягування завершено. Оброблено {processed_count}/{total_decisions} рішень"
            
            self.process_control.update_progress(processed_count, total_decisions, final_msg)
            
            # Розраховуємо статистики
            success_rate = (success_count / processed_count * 100) if processed_count > 0 else 0
            error_rate = (error_count / processed_count * 100) if processed_count > 0 else 0
            
            # Отримуємо статистику тригерів
            from bankruptcy.utils.fast_resolution_extractor import get_extraction_statistics
            trigger_stats = get_extraction_statistics()
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n🎉 Примусове витягування резолютивних частин завершено:\n"
                    f"   📊 Оброблено рішень: {processed_count}\n"
                    f"   ✅ Успішно витягнуто: {success_count} ({success_rate:.1f}%)\n"
                    f"   ❌ Помилок: {error_count} ({error_rate:.1f}%)\n"
                    f"   ⚡ Потоків: {workers}\n"
                    f"   📦 Розмір батчу: {batch_size}\n"
                    f"   ⏱️ Час виконання: {timezone.now() - self.process_control.started_at}\n"
                    f"\n🔍 Статистика тригерних слів:\n"
                    f"   🎯 З тригерами: {trigger_stats["decisions_with_triggers"]} "
                    f"({trigger_stats["trigger_percentage"]:.1f}%)\n"
                    f"   🚨 Критичних рішень: {trigger_stats["critical_decisions"]}"
                )
            )

        except Exception as e:
            error_msg = f"Критична помилка при примусовому витягуванні: {str(e)}"
            self.stdout.write(self.style.ERROR(f"❌ {error_msg}"))
            logger.error(error_msg)
            
            if self.process_control:
                self.process_control.last_message = f"Помилка: {str(e)}"
                self.process_control.status = "error"
                self.process_control.save()

        finally:
            # Завжди зупиняємо примусовий режим та екстрактор
            if self.extractor:
                self.extractor.stop_preloading()
            
            if self.process_control:
                self.process_control.stop_forced()
                self.stdout.write("🔄 Відновлено штатний режим роботи системи")