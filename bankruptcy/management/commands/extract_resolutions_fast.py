from django.core.management.base import BaseCommand
from django.utils import timezone
from bankruptcy.utils.fast_resolution_extractor import FastResolutionExtractor, get_extraction_statistics
from bankruptcy.models import TrackedCourtDecision
import time


class Command(BaseCommand):
    help = "Швидке витягування резолютивних частин з судових рішень (на основі SR_AI алгоритмів)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=500,
            help="Максимальна кількість рішень для обробки за один запуск"
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            help="Безперервне витягування резолютивних частин"
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Розмір батчу для обробки"
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=50,
            help="Максимальна кількість потоків для обробки"
        )
        parser.add_argument(
            "--stats",
            action="store_true",
            help="Показати статистику витягування резолютивних частин"
        )
        parser.add_argument(
            "--case-id",
            type=int,
            help="Обробити тільки рішення для конкретної справи"
        )

    def handle(self, *args, **options):
        if options["stats"]:
            self.show_statistics()
            return

        # Створюємо екземпляр швидкого екстрактора
        extractor = FastResolutionExtractor()
        
        # Налаштовуємо параметри
        if options["batch_size"]:
            extractor.batch_size = options["batch_size"]
        if options["max_workers"]:
            extractor.max_workers = options["max_workers"]

        self.stdout.write(
            self.style.SUCCESS(
                f"Запуск швидкого витягування резолютивних частин "
                f"(батч: {extractor.batch_size}, потоків: {extractor.max_workers})"
            )
        )

        if options["continuous"]:
            self.handle_continuous_extraction(extractor)
        elif options["case_id"]:
            self.handle_case_extraction(extractor, options["case_id"])
        else:
            self.handle_batch_extraction(extractor, options["limit"])

    def handle_batch_extraction(self, extractor, limit):
        """
        Обробляє витягування резолютивних частин в режимі батчу
        """
        start_time = time.time()
        
        # Показуємо початкову статистику
        stats = get_extraction_statistics()
        self.stdout.write(
            f"Початкова статистика: {stats["extracted_decisions"]}/{stats["total_decisions"]} "
            f"рішень мають резолютивні частини ({stats["extraction_percentage"]:.1f}%)"
        )
        self.stdout.write(f"Рішень для обробки: {stats["pending_decisions"]}")
        self.stdout.write(
            f"З тригерами: {stats["decisions_with_triggers"]} "
            f"({stats["trigger_percentage"]:.1f}% від витягнутих)"
        )

        # Виконуємо витягування
        result = extractor.extract_resolutions_batch(limit)

        # Показуємо результати
        end_time = time.time()
        duration = end_time - start_time

        if result["success"]:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nВитягування завершено за {duration:.2f} секунд:\n"
                    f"- Оброблено рішень: {result["processed"]}\n"
                    f"- Успішно витягнуто: {result["successful"]}\n"
                    f"- Невдало: {result["failed"]}"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(f"Помилка: {result.get("error", "Невідома помилка")}")
            )

        # Показуємо фінальну статистику
        final_stats = get_extraction_statistics()
        self.stdout.write(
            f"\nФінальна статистика: {final_stats["extracted_decisions"]}/{final_stats["total_decisions"]} "
            f"рішень мають резолютивні частини ({final_stats["extraction_percentage"]:.1f}%)"
        )
        self.stdout.write(f"Залишилось для обробки: {final_stats["pending_decisions"]}")
        self.stdout.write(
            f"З тригерами: {final_stats["decisions_with_triggers"]} "
            f"({final_stats["trigger_percentage"]:.1f}% від витягнутих)"
        )
        if final_stats["critical_decisions"] > 0:
            self.stdout.write(
                self.style.WARNING(f"КРИТИЧНИХ рішень: {final_stats["critical_decisions"]}")
            )

    def handle_case_extraction(self, extractor, case_id):
        """
        Обробляє витягування резолютивних частин для конкретної справи
        """
        from bankruptcy.models import TrackedBankruptcyCase
        from django.db import models
        
        try:
            case = TrackedBankruptcyCase.objects.get(id=case_id)
        except TrackedBankruptcyCase.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"Справа з ID {case_id} не знайдена")
            )
            return
        
        self.stdout.write(
            self.style.SUCCESS(f"Витягування резолютивних частин для справи {case.case_number} (ID: {case_id})")
        )
        
        # Знаходимо рішення без резолютивного тексту для цієї справи
        decisions_to_process = TrackedCourtDecision.objects.filter(
            tracked_case_id=case_id
        ).filter(
            doc_url__isnull=False
        ).filter(
            models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        )
        
        count = decisions_to_process.count()
        if count == 0:
            self.stdout.write("Немає рішень для обробки в цій справі")
            return
        
        self.stdout.write(f"Знайдено {count} рішень для обробки")
        
        # Використовуємо метод batch обробки з обмеженням на цю справу
        start_time = time.time()
        result = extractor.extract_resolutions_for_case(case_id, count)
        duration = time.time() - start_time
        
        if result.get("success", False):
            self.stdout.write(
                self.style.SUCCESS(
                    f"Витягування завершено за {duration:.2f} секунд:\n"
                    f"- Оброблено рішень: {result["processed"]}\n"
                    f"- Успішно витягнуто: {result["successful"]}\n"
                    f"- Невдало: {result["failed"]}"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(f"Помилка: {result.get("error", "Невідома помилка")}")
            )

    def handle_continuous_extraction(self, extractor):
        """
        Обробляє безперервне витягування резолютивних частин
        """
        self.stdout.write(
            self.style.SUCCESS("Запуск безперервного витягування резолютивних частин...")
        )
        
        try:
            cycle_count = 0
            while True:
                cycle_count += 1
                cycle_start_time = time.time()
                
                self.stdout.write(f"\n=== ЦИКЛ {cycle_count} ===")
                
                # Перевіряємо статистику
                stats = get_extraction_statistics()
                self.stdout.write(
                    f"Статистика: {stats["extracted_decisions"]}/{stats["total_decisions"]} "
                    f"рішень мають резолютивні частини ({stats["extraction_percentage"]:.1f}%)"
                )
                
                if stats["pending_decisions"] == 0:
                    self.stdout.write("Всі резолютивні частини витягнуто. Очікування нових рішень...")
                    time.sleep(60)
                    continue
                
                self.stdout.write(f"Знайдено {stats["pending_decisions"]} рішень для обробки")
                
                # Виконуємо витягування
                result = extractor.extract_resolutions_batch()
                
                cycle_duration = time.time() - cycle_start_time
                
                if result["success"] and result["processed"] > 0:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Цикл {cycle_count} завершено за {cycle_duration:.2f}с: "
                            f"оброблено {result["processed"]}, "
                            f"успішно {result["successful"]}, "
                            f"невдало {result["failed"]}"
                        )
                    )
                else:
                    self.stdout.write(f"Цикл {cycle_count}: немає рішень для обробки")
                    time.sleep(30)
                
                # Невелика пауза між циклами
                time.sleep(5)
                
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING("\nБезперервне витягування зупинено користувачем")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Помилка в безперервному витягуванні: {e}")
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
        
        decisions_not_found = TrackedCourtDecision.objects.filter(
            resolution_text__icontains="не знайдена"
        ).count()
        
        decisions_download_failed = TrackedCourtDecision.objects.filter(
            resolution_text__icontains="Не вдалося завантажити"
        ).count()

        self.stdout.write(
            self.style.SUCCESS("=== СТАТИСТИКА ВИТЯГУВАННЯ РЕЗОЛЮТИВНИХ ЧАСТИН ===")
        )
        self.stdout.write(f"Загальна кількість рішень: {stats["total_decisions"]}")
        self.stdout.write(f"Рішень з резолютивними частинами: {stats["extracted_decisions"]}")
        self.stdout.write(f"Рішень без резолютивних частин: {stats["pending_decisions"]}")
        self.stdout.write(f"Прогрес витягування: {stats["extraction_percentage"]:.1f}%")
        self.stdout.write(f"Витягнуто за останню годину: {recent_extractions}")
        self.stdout.write(
            self.style.SUCCESS(f"\n=== СТАТИСТИКА ТРИГЕРІВ ===")
        )
        self.stdout.write(f"Рішень з тригерними словами: {stats["decisions_with_triggers"]}")
        self.stdout.write(f"Відсоток з тригерами: {stats["trigger_percentage"]:.1f}%")
        self.stdout.write(f"Критичних рішень: {stats["critical_decisions"]}")
        self.stdout.write(f"Резолютивних тригерів: {stats["resolution_triggers"]}")
        
        self.stdout.write(
            self.style.WARNING("\n=== СТАТИСТИКА ПОМИЛОК ===")
        )
        self.stdout.write(f"Рішень з помилками обробки: {decisions_with_errors}")
        self.stdout.write(f"Рішень де резолютивна частина не знайдена: {decisions_not_found}")
        self.stdout.write(f"Рішень з помилками завантаження: {decisions_download_failed}")
        
        # Показуємо найсвіжіші успішні витягування
        recent_successful = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__isnull=False,
            resolution_text__isnull=False
        ).exclude(
            resolution_text__icontains="Помилка"
        ).exclude(
            resolution_text__icontains="не знайдена"
        ).exclude(
            resolution_text__icontains="Не вдалося"
        ).order_by("-resolution_extracted_at")[:5]
        
        if recent_successful:
            self.stdout.write(
                self.style.SUCCESS("\n=== ОСТАННІ УСПІШНІ ВИТЯГУВАННЯ ===")
            )
            for decision in recent_successful:
                self.stdout.write(
                    f"ID: {decision.id}, "
                    f"Дата: {decision.resolution_extracted_at.strftime("%d.%m.%Y %H:%M")}, "
                    f"Текст: {decision.resolution_text[:100]}..."
                )