"""
Команда для аналізу резолютивних частин через другий Mistral API з дедуплікацією.
"""
import logging
import time
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from bankruptcy.models import (
    TrackedCourtDecision,
    BankruptcyCase,
    DeduplicationProcessStats,
    LLMAnalysisLog
)
from bankruptcy.services.mistral_dedup_service import MistralDeduplicationService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Аналізує резолютивні частини через другий Mistral API з дедуплікацією"

    def add_arguments(self, parser):
        parser.add_argument(
            "--case-number",
            type=str,
            help="Номер конкретної справи для обробки"
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10000,
            help="Максимальна кількість справ для обробки (за замовчуванням: 10000)"
        )
        parser.add_argument(
            "--start-from",
            type=int,
            help="Почати обробку зі справи з певним номером (number)"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Примусова обробка навіть якщо справа вже оброблена"
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="Тестовий режим - тільки перевірка з\"єднання"
        )
        parser.add_argument(
            "--continuous",
            action="store_true",
            help="Циклічна обробка - повторює обробку справ безперервно"
        )

    def handle(self, *args, **options):
        self.dedup_service = MistralDeduplicationService()
        self.stats = DeduplicationProcessStats.get_current_stats()

        # Тестовий режим
        if options["test"]:
            self.test_connection()
            return

        # Конкретна справа
        if options["case_number"]:
            self.process_single_case(options["case_number"], options["force"])
            return

        # Масова обробка
        if options["continuous"]:
            self.process_continuous(options["limit"], options["force"], options.get("start_from"))
        else:
            self.process_batch(options["limit"], options["force"], options.get("start_from"))

    def test_connection(self):
        """Тестує з'єднання з другим Mistral API."""
        self.stdout.write("Тестування з'єднання з другим Mistral API...")

        if self.dedup_service.test_connection():
            self.stdout.write(
                self.style.SUCCESS("✅ З'єднання з другим Mistral API успішне!")
            )
        else:
            self.stdout.write(
                self.style.ERROR("❌ Помилка з'єднання з другим Mistral API")
            )

    def process_continuous(self, limit, force=False, start_from=None):
        """Циклічна обробка резолютивних частин з дедуплікацією."""
        start_msg = f" починаючи зі справи {start_from}" if start_from else ""
        self.stdout.write(f"🔄 Циклічна дедуплікація резолютивних частин (ліміт за цикл: {limit}){start_msg}")

        self.stats.start_processing()
        cycle = 0
        total_processed = 0

        try:
            while True:
                cycle += 1
                self.stdout.write(f"\n🔄 Цикл дедуплікації #{cycle}")

                # Знаходимо справи для обробки (фокусуємося на необроблених)
                decisions_to_process = self.get_decisions_to_process(limit, force, start_from)

                if not decisions_to_process:
                    self.stdout.write("✅ Всі доступні рішення оброблені в цьому циклі")
                    self.stdout.write("😴 Чекаємо 60 секунд перед наступним циклом дедуплікації...")
                    time.sleep(60)  # Більша затримка для другого процесу
                    continue

                self.stdout.write(f"📋 До дедуплікації: {len(decisions_to_process)} рішень")

                cycle_processed = 0
                for i, decision in enumerate(decisions_to_process, 1):
                    try:
                        self.stdout.write(f"[{i}/{len(decisions_to_process)}] Дедуплікація {decision.doc_id}")
                        result = self.analyze_decision_with_dedup(decision, force)

                        if result.get("success"):
                            cycle_processed += 1
                            total_processed += 1

                        # Затримка між запитами для другого API
                        time.sleep(4)  # 4 секунди затримка

                    except Exception as e:
                        logger.error(f"Помилка дедуплікації рішення {decision.doc_id}: {e}")
                        self.stats.update_stats(error=str(e))
                        continue

                self.stdout.write(
                    self.style.SUCCESS(f"✅ Цикл #{cycle}: дедупліковано {cycle_processed} рішень. Загалом: {total_processed}")
                )

                # Перерва між циклами
                self.stdout.write("⏱️ Перерва 15 секунд між циклами дедуплікації...")
                time.sleep(15)

        except KeyboardInterrupt:
            self.stdout.write(
                self.style.SUCCESS(f"\n⏹️ Дедуплікацію зупинено користувачем. Загалом оброблено {total_processed} рішень за {cycle} циклів.")
            )
        finally:
            self.stats.stop_processing()

    def process_batch(self, limit, force=False, start_from=None):
        """Пакетна обробка з дедуплікацією."""
        start_msg = f" починаючи зі справи {start_from}" if start_from else ""
        self.stdout.write(f"Дедуплікація резолютивних частин (ліміт: {limit}){start_msg}")

        self.stats.start_processing()

        try:
            decisions_to_process = self.get_decisions_to_process(limit, force, start_from)

            if not decisions_to_process:
                self.stdout.write(
                    self.style.SUCCESS("Всі доступні рішення вже дедупліковані")
                )
                return

            self.stdout.write(f"До дедуплікації: {len(decisions_to_process)} рішень")

            processed = 0
            for decision in decisions_to_process:
                try:
                    result = self.analyze_decision_with_dedup(decision, force)
                    if result.get("success"):
                        processed += 1

                    # Затримка між запитами
                    time.sleep(4)

                except Exception as e:
                    logger.error(f"Помилка дедуплікації рішення {decision.doc_id}: {e}")
                    self.stats.update_stats(error=str(e))
                    continue

            self.stdout.write(
                self.style.SUCCESS(f"Успішно дедупліковано {processed} рішень")
            )

        finally:
            self.stats.stop_processing()

    def get_decisions_to_process(self, limit, force=False, start_from=None):
        """Отримує список рішень для дедуплікації."""
        # Фокусуємося на рішеннях які ще не були дедупліковані
        # або на тих, де можуть бути дублікати (підсумкові ухвали, повні версії)

        query = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False,
            has_trigger_words=True
        ).exclude(resolution_text="")

        # Пріоритет: рішення з ключовими словами що вказують на можливі дублікати
        priority_keywords = [
            "підсумками попереднього засідання",
            "перераховуються",
            "визнано грошові вимоги",
            "повна версія",
            "додатково",
        ]

        priority_query = query.filter(
            resolution_text__iregex=r'|'.join(priority_keywords)
        )

        if start_from:
            priority_query = priority_query.filter(
                tracked_case__bankruptcy_case__number__gte=start_from
            )

        # Порядок: спочатку пріоритетні, потім інші
        priority_results = list(priority_query.order_by("-tracked_case__bankruptcy_case__number")[:limit//2])

        # Додаємо звичайні рішення, що залишилися
        remaining_limit = limit - len(priority_results)
        if remaining_limit > 0:
            processed_case_ids = [r.tracked_case.bankruptcy_case_id for r in priority_results]

            remaining_query = query.exclude(
                tracked_case__bankruptcy_case_id__in=processed_case_ids
            ).exclude(
                resolution_text__iregex=r'|'.join(priority_keywords)
            )

            if start_from:
                remaining_query = remaining_query.filter(
                    tracked_case__bankruptcy_case__number__gte=start_from
                )

            remaining_results = list(remaining_query.order_by("-tracked_case__bankruptcy_case__number")[:remaining_limit])
            priority_results.extend(remaining_results)

        return priority_results

    def analyze_decision_with_dedup(self, decision, force=False):
        """Аналізує рішення з дедуплікацією."""
        case = decision.tracked_case.bankruptcy_case
        start_time = time.time()

        try:
            self.stdout.write(f"🔍 Дедуплікація рішення {decision.doc_id} для справи {case.case_number}")

            # Викликаємо сервіс дедуплікації
            result = self.dedup_service.analyze_resolutive_part_with_dedup(
                decision.resolution_text, case
            )

            processing_time = time.time() - start_time

            if "error" in result:
                self.stdout.write(
                    self.style.ERROR(f"❌ Помилка дедуплікації: {result['error']}")
                )
                self.stats.update_stats(error=result["error"], processing_time=processing_time)
                return result

            # Обробляємо результати дедуплікації
            dedup_stats = result.get("deduplication", {})
            doc_type = result.get("analysis", {}).get("document_type", "")

            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ {dedup_stats.get('message', 'Дедуплікація завершена')} "
                    f"(тип: {doc_type})"
                )
            )

            # Оновлюємо статистику
            self.stats.update_stats(
                cases_processed=1,
                creditors_added=dedup_stats.get("added_creditors", 0),
                duplicates_removed=dedup_stats.get("duplicates_removed", 0),
                claims_updated=dedup_stats.get("updated_claims", 0),
                doc_type=doc_type,
                processing_time=processing_time
            )

            return {"success": True, "result": result}

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Помилка дедуплікації рішення {decision.doc_id}: {e}")
            self.stats.update_stats(error=str(e), processing_time=processing_time)
            raise

    def process_single_case(self, case_number, force=False):
        """Обробляє рішення для конкретної справи."""
        try:
            # Знаходимо справу
            try:
                case = BankruptcyCase.objects.get(number=int(case_number))
            except (ValueError, BankruptcyCase.DoesNotExist):
                case = BankruptcyCase.objects.get(case_number=case_number)

            self.stdout.write(f"Дедуплікація справи: {case.case_number} (№{case.number})")

            # Знаходимо всі рішення цієї справи
            decisions = TrackedCourtDecision.objects.filter(
                tracked_case__bankruptcy_case=case,
                resolution_text__isnull=False,
                has_trigger_words=True
            ).exclude(resolution_text="")

            if not decisions.exists():
                self.stdout.write(
                    self.style.WARNING(f"Не знайдено рішень для дедуплікації у справі {case_number}")
                )
                return

            self.stdout.write(f"Знайдено {decisions.count()} рішень для дедуплікації")

            self.stats.start_processing()
            try:
                for decision in decisions:
                    self.analyze_decision_with_dedup(decision, force)
            finally:
                self.stats.stop_processing()

        except BankruptcyCase.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"Справа {case_number} не знайдена")
            )