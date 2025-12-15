from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import models
from bankruptcy.utils.fast_resolution_extractor import FastResolutionExtractor, get_extraction_statistics
from bankruptcy.models import TrackedCourtDecision
import time


class Command(BaseCommand):
    help = "УЛЬТРА-ШВИДКЕ витягування резолютивних частин з максимальною оптимізацією"

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=None,  # БЕЗ ЛІМІТУ за замовчуванням - обробляти ВСІ рішення
            help="Максимальна кількість рішень для обробки за один запуск (за замовчуванням - всі)"
        )
        parser.add_argument(
            "--missing-only",
            action="store_true",
            help="Обробляти тільки рішення без резолютивного тексту"
        )
        parser.add_argument(
            "--auto-incremental",
            action="store_true", 
            help="Автоматично переключатись на інкрементальний режим"
        )
        parser.add_argument(
            "--ultra-mode",
            action="store_true",
            help="УЛЬТРА-ШВИДКИЙ режим з максимальними параметрами продуктивності"
        )
        parser.add_argument(
            "--stats",
            action="store_true",
            help="Показати статистику витягування"
        )

    def handle(self, *args, **options):
        if options["stats"]:
            self.show_statistics()
            return

        start_time = time.time()
        
        # Обробка переривань для коректного завершення
        import signal
        import sys
        
        def signal_handler(signum, frame):
            """Обробник сигналу для коректного завершення процесу"""
            self.stdout.write(self.style.WARNING("\n🔴 ОТРИМАНО СИГНАЛ ПЕРЕРИВАННЯ - завершення процесу..."))
            try:
                process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                process_control.status = "stopped"
                process_control.last_message = "🔴 Процес зупинено користувачем"
                process_control.finished_at = timezone.now()
                process_control.save()
                self.stdout.write("✅ Статус процесу оновлено")
            except Exception as e:
                self.stdout.write(f"⚠️ Помилка оновлення статусу: {e}")
            sys.exit(0)
        
        # Реєструємо обробники сигналів
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Створюємо екземпляр УЛЬТРА-ШВИДКОГО екстрактора
        extractor = FastResolutionExtractor()
        
        # УЛЬТРА-РЕЖИМ: максимальні параметри продуктивності
        if options["ultra_mode"]:
            extractor.max_workers = 15  # УЛЬТРА: 15 потоків для ультра швидкості витягування
            extractor.batch_size = 2000  # КРИТИЧНО: максимум за раз
            extractor.download_timeout = 10  # КРИТИЧНО: швидкі таймаути
            extractor.request_delay = 0.005  # КРИТИЧНО: мінімальна затримка
            
            self.stdout.write(
                self.style.SUCCESS(
                    "🚀 АКТИВОВАНО УЛЬТРА-РЕЖИМ:\n"
                    f"   - Потоків: {extractor.max_workers}\n"
                    f"   - Батч: {extractor.batch_size}\n"
                    f"   - Таймаут: {extractor.download_timeout}с\n"
                    f"   - Затримка: {extractor.request_delay}с"
                )
            )

        # Показуємо початкову статистику
        stats = get_extraction_statistics()
        self.stdout.write(
            self.style.SUCCESS(
                f"📊 ПОЧАТКОВА СТАТИСТИКА:\n"
                f"   - Всього рішень: {stats["total_decisions"]}\n"
                f"   - З резолютивними: {stats["extracted_decisions"]} ({stats["extraction_percentage"]:.1f}%)\n"
                f"   - До обробки: {stats["pending_decisions"]}\n"
                f"   - З тригерами: {stats["decisions_with_triggers"]} ({stats["trigger_percentage"]:.1f}%)"
            )
        )

        # Визначаємо режим роботи
        if options["missing_only"] or options["auto_incremental"]:
            # ІНКРЕМЕНТАЛЬНИЙ РЕЖИМ - тільки нові рішення
            self.stdout.write("⚡ Інкрементальний режим: обробка тільки рішень без резолютивного тексту")
            
            decisions_query = TrackedCourtDecision.objects.filter(
                models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
            ).filter(
                doc_url__isnull=False
            ).exclude(
                doc_url__exact=""
            ).exclude(
                doc_url__exact="nan"
            ).order_by("-found_at")
            
            # Застосовуємо ліміт тільки якщо він заданий і більше 0
            if options.get("limit") and options["limit"] > 0:
                decisions_to_process = decisions_query[:options["limit"]]
            else:
                decisions_to_process = decisions_query
            
        else:
            # ПОВНИЙ РЕЖИМ - всі рішення без резолютивного тексту
            self.stdout.write("🔥 Повний режим: обробка всіх рішень без резолютивного тексту")
            
            decisions_query = TrackedCourtDecision.objects.filter(
                doc_url__isnull=False
            ).filter(
                models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
            ).exclude(
                doc_url__exact=""
            ).exclude(
                doc_url__exact="nan"
            ).order_by("-found_at")
            
            # Застосовуємо ліміт тільки якщо він заданий і більше 0
            if options.get("limit") and options["limit"] > 0:
                decisions_to_process = decisions_query[:options["limit"]]
            else:
                decisions_to_process = decisions_query

        if not decisions_to_process:
            self.stdout.write(
                self.style.WARNING("✅ Всі рішення вже оброблені!")
            )
            return

        decisions_list = list(decisions_to_process)
        self.stdout.write(
            self.style.SUCCESS(
                f"🎯 ЗНАЙДЕНО {len(decisions_list)} РІШЕНЬ ДЛЯ ОБРОБКИ"
            )
        )

        # Оновлюємо статус процесу перед початком
        try:
            from bankruptcy.models import SystemProcessControl
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            process_control.update_progress(0, len(decisions_list), f"🚀 Початок обробки {len(decisions_list):,} рішень...")
            process_control.status = "running"
            process_control.save()
        except SystemProcessControl.DoesNotExist:
            pass

        # ЗАПУСКАЄМО УЛЬТРА-ШВИДКЕ ВИТЯГУВАННЯ
        self.stdout.write("🚀 ЗАПУСК УЛЬТРА-ШВИДКОГО ВИТЯГУВАННЯ...")
        
        extraction_start = time.time()
        
        # Використовуємо кастомний метод для переданого списку з callback для оновлення прогресу
        def progress_callback(processed, total, successful):
            try:
                process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                process_control.update_progress(processed, total, f"🚀 Обробка: {processed}/{total}, успішно: {successful}")
                process_control.save()
            except SystemProcessControl.DoesNotExist:
                pass

        result = extractor.extract_resolutions_batch_custom(decisions_list, progress_callback=progress_callback)
        
        extraction_duration = time.time() - extraction_start
        total_duration = time.time() - start_time

        # Оновлюємо статус після завершення
        try:
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            if result["success"]:
                process_control.update_progress(
                    result["processed"], 
                    len(decisions_list), 
                    f"🚀 УЛЬТРА-РЕЖИМ завершено: {result["successful"]}/{result["processed"]} успішно"
                )
                process_control.status = "idle"
            else:
                process_control.status = "failed"
                process_control.last_message = f"❌ Помилка: {result.get("error", "Невідома помилка")}"
            process_control.save()
        except SystemProcessControl.DoesNotExist:
            pass

        # РЕЗУЛЬТАТИ
        if result["success"]:
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ УЛЬТРА-ШВИДКЕ ВИТЯГУВАННЯ ЗАВЕРШЕНО:\n"
                    f"   - Час витягування: {extraction_duration:.1f}с\n" 
                    f"   - Загальний час: {total_duration:.1f}с\n"
                    f"   - Оброблено рішень: {result["processed"]}\n"
                    f"   - Успішно витягнуто: {result["successful"]}\n"
                    f"   - Помилок: {result["failed"]}\n"
                    f"   - Швидкість: {result["processed"] / extraction_duration:.1f} рішень/сек"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(f"❌ ПОМИЛКА: {result.get("error", "Невідома помилка")}")
            )

        # Показуємо фінальну статистику
        final_stats = get_extraction_statistics()
        improvement = final_stats["extracted_decisions"] - stats["extracted_decisions"]
        
        self.stdout.write(
            self.style.SUCCESS(
                f"📈 ФІНАЛЬНА СТАТИСТИКА:\n"
                f"   - Всього рішень: {final_stats["total_decisions"]}\n"
                f"   - З резолютивними: {final_stats["extracted_decisions"]} ({final_stats["extraction_percentage"]:.1f}%)\n"
                f"   - Покращення: +{improvement} рішень\n"
                f"   - Залишилось: {final_stats["pending_decisions"]}\n"
                f"   - З тригерами: {final_stats["decisions_with_triggers"]} ({final_stats["trigger_percentage"]:.1f}%)"
            )
        )
        
        if final_stats.get("critical_decisions", 0) > 0:
            self.stdout.write(
                self.style.WARNING(f"⚠️ КРИТИЧНИХ рішень: {final_stats["critical_decisions"]}")
            )

    def show_statistics(self):
        """
        Показує детальну статистику витягування резолютивних частин
        """
        stats = get_extraction_statistics()
        
        # Додаткова статистика
        recent_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=timezone.now() - timezone.timedelta(hours=1)
        ).count()
        
        decisions_with_errors = TrackedCourtDecision.objects.filter(
            resolution_text__icontains="Помилка"
        ).count()

        self.stdout.write(
            self.style.SUCCESS("=== 📊 СТАТИСТИКА УЛЬТРА-ШВИДКОГО ВИТЯГУВАННЯ ===")
        )
        self.stdout.write(f"📋 Загальна кількість рішень: {stats["total_decisions"]}")
        self.stdout.write(f"✅ Рішень з резолютивними частинами: {stats["extracted_decisions"]}")
        self.stdout.write(f"⏳ Рішень без резолютивних частин: {stats["pending_decisions"]}")
        self.stdout.write(f"📈 Прогрес витягування: {stats["extraction_percentage"]:.1f}%")
        self.stdout.write(f"⚡ Витягнуто за останню годину: {recent_extractions}")
        
        self.stdout.write(
            self.style.SUCCESS(f"\n=== 🎯 СТАТИСТИКА ТРИГЕРІВ ===")
        )
        self.stdout.write(f"🔍 Рішень з тригерними словами: {stats["decisions_with_triggers"]}")
        self.stdout.write(f"📊 Відсоток з тригерами: {stats["trigger_percentage"]:.1f}%")
        self.stdout.write(f"⚠️ Критичних рішень: {stats["critical_decisions"]}")
        self.stdout.write(f"⚖️ Резолютивних тригерів: {stats["resolution_triggers"]}")
        
        self.stdout.write(
            self.style.WARNING("\n=== ❌ СТАТИСТИКА ПОМИЛОК ===")
        )
        self.stdout.write(f"🔴 Рішень з помилками обробки: {decisions_with_errors}")

        # Рекомендації щодо швидкості
        if stats["pending_decisions"] > 1000:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n=== 🚀 РЕКОМЕНДАЦІЇ ДЛЯ УЛЬТРА-ШВИДКОСТІ ===\n"
                    f"Для обробки {stats["pending_decisions"]} рішень використовуйте:\n"
                    f"python manage.py extract_resolution_texts_ultra_fast --ultra-mode --limit 5000"
                )
            )