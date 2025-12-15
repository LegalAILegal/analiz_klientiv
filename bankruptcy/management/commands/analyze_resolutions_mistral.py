"""
Команда для аналізу резолютивних частин через Mistral AI та витягування даних про кредиторів.
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
    Creditor,
    CreditorClaim,
    LLMAnalysisLog
)
from bankruptcy.services.mistral_service import MistralAnalysisService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Аналізує резолютивні частини через Mistral AI для витягування даних про кредиторів"

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
        self.mistral_service = MistralAnalysisService()

        # Тестовий режим
        if options["test"]:
            self.test_mistral_connection()
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

    def test_mistral_connection(self):
        """Тестує з"єднання з Mistral API."""
        self.stdout.write("Тестування з\"єднання з Mistral API...")

        if self.mistral_service.test_connection():
            self.stdout.write(
                self.style.SUCCESS("✅ З\"єднання з Mistral API успішне!")
            )
        else:
            self.stdout.write(
                self.style.ERROR("❌ Помилка з\"єднання з Mistral API")
            )

    def process_single_case(self, case_number, force=False):
        """Обробляє резолютивні частини для конкретної справи."""
        try:
            # Спробуємо знайти за номером (number) або номером справи (case_number)
            try:
                case = BankruptcyCase.objects.get(number=int(case_number))
            except (ValueError, BankruptcyCase.DoesNotExist):
                case = BankruptcyCase.objects.get(case_number=case_number)

            self.stdout.write(f"Обробка справи: {case.case_number} (№{case.number})")

            # Знаходимо судові рішення з резолютивними частинами та тригерними словами
            decisions = TrackedCourtDecision.objects.filter(
                tracked_case__bankruptcy_case=case,
                resolution_text__isnull=False,
                has_trigger_words=True
            ).exclude(resolution_text="")

            if not decisions.exists():
                self.stdout.write(
                    self.style.WARNING(f"Не знайдено рішень з резолютивними частинами для справи {case_number}")
                )
                return

            self.stdout.write(f"Знайдено {decisions.count()} рішень для аналізу")

            for decision in decisions:
                self.analyze_decision_resolution(decision, force)

        except BankruptcyCase.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"Справа {case_number} не знайдена")
            )

    def process_continuous(self, limit, force=False, start_from=None):
        """Циклічна обробка резолютивних частин."""
        start_msg = f" починаючи зі справи {start_from}" if start_from else ""
        self.stdout.write(f"🔄 Циклічна обробка резолютивних частин (ліміт за цикл: {limit}){start_msg}")

        cycle = 0
        total_processed = 0

        try:
            while True:
                cycle += 1
                self.stdout.write(f"\n🔄 Цикл #{cycle}")

                # Знаходимо справи з необробленими резолютивними частинами
                decisions_to_process = self.get_decisions_to_process(limit, force, start_from)

                if not decisions_to_process:
                    self.stdout.write("✅ Всі доступні резолютивні частини оброблені в цьому циклі")
                    if not force:
                        self.stdout.write("😴 Чекаємо 30 секунд перед наступним циклом...")
                        time.sleep(30)  # Чекаємо 30 секунд перед наступним циклом
                        continue
                    else:
                        self.stdout.write("🔄 Force режим - перерва 5 секунд...")
                        time.sleep(5)
                        continue

                self.stdout.write(f"📋 До обробки: {len(decisions_to_process)} рішень")

                cycle_processed = 0
                for i, decision in enumerate(decisions_to_process, 1):
                    try:
                        self.stdout.write(f"[{i}/{len(decisions_to_process)}] Обробка {decision.doc_id}")
                        self.analyze_decision_resolution(decision, force)
                        cycle_processed += 1
                        total_processed += 1

                        # Затримка між запитами (менша для швидкої моделі ministral-8b)
                        time.sleep(3)  # Зменшено для швидшої моделі

                    except Exception as e:
                        logger.error(f"Помилка обробки рішення {decision.doc_id}: {e}")
                        continue

                self.stdout.write(
                    self.style.SUCCESS(f"✅ Цикл #{cycle}: оброблено {cycle_processed} рішень. Загалом: {total_processed}")
                )

                # Коротка перерва між циклами
                self.stdout.write("⏱️ Перерва 10 секунд між циклами...")
                time.sleep(10)

        except KeyboardInterrupt:
            self.stdout.write(
                self.style.SUCCESS(f"\n⏹️ Зупинено користувачем. Загалом оброблено {total_processed} рішень за {cycle} циклів.")
            )

    def process_batch(self, limit, force=False, start_from=None):
        """Масова обробка резолютивних частин."""
        start_msg = f" починаючи зі справи {start_from}" if start_from else ""
        self.stdout.write(f"Масова обробка резолютивних частин (ліміт: {limit}){start_msg}")

        # Знаходимо справи з необробленими резолютивними частинами
        decisions_to_process = self.get_decisions_to_process(limit, force, start_from)

        if not decisions_to_process:
            self.stdout.write(
                self.style.SUCCESS("Всі доступні резолютивні частини вже оброблені")
            )
            return

        self.stdout.write(f"До обробки: {len(decisions_to_process)} рішень")

        processed = 0
        for decision in decisions_to_process:
            try:
                self.analyze_decision_resolution(decision, force)
                processed += 1

                # Затримка між запитами (менша для швидкої моделі ministral-8b)
                time.sleep(3)  # Зменшено для швидшої моделі

            except Exception as e:
                logger.error(f"Помилка обробки рішення {decision.doc_id}: {e}")
                continue

        self.stdout.write(
            self.style.SUCCESS(f"Успішно оброблено {processed} рішень")
        )

    def get_decisions_to_process(self, limit, force=False, start_from=None):
        """Отримує список рішень для обробки."""
        # Пріоритет: рішення з фразою "визнати грошові вимоги" або "кредиторські вимоги"
        priority_query = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False,
            has_trigger_words=True,
            resolution_text__icontains="визнати"
        ).filter(
            Q(resolution_text__icontains="грошові вимоги") |
            Q(resolution_text__icontains="кредиторські вимоги")
        ).exclude(resolution_text="")

        # Фільтруємо за номером справи, якщо вказано start_from
        if start_from:
            priority_query = priority_query.filter(
                tracked_case__bankruptcy_case__number__gte=start_from
            )

        if not force:
            # Виключаємо вже оброблені (ті що мають записи в CreditorClaim)
            processed_cases = CreditorClaim.objects.values_list(
                "case_id", flat=True
            ).distinct()

            priority_query = priority_query.exclude(
                tracked_case__bankruptcy_case_id__in=processed_cases
            )

        # Спочатку пріоритетні рішення
        priority_results = list(priority_query.order_by("-tracked_case__bankruptcy_case__number")[:limit])

        # Якщо не вистачає, додаємо інші рішення з тригерними словами
        if len(priority_results) < limit:
            remaining_limit = limit - len(priority_results)
            processed_case_ids = [r.tracked_case.bankruptcy_case_id for r in priority_results]

            fallback_query = TrackedCourtDecision.objects.filter(
                resolution_text__isnull=False,
                has_trigger_words=True
            ).exclude(resolution_text="").exclude(
                tracked_case__bankruptcy_case_id__in=processed_case_ids
            )

            if start_from:
                fallback_query = fallback_query.filter(
                    tracked_case__bankruptcy_case__number__gte=start_from
                )

            if not force:
                fallback_query = fallback_query.exclude(
                    tracked_case__bankruptcy_case_id__in=processed_cases
                )

            fallback_results = list(fallback_query.order_by("-tracked_case__bankruptcy_case__number")[:remaining_limit])
            priority_results.extend(fallback_results)

        return priority_results

    def analyze_decision_resolution(self, decision, force=False):
        """Аналізує резолютивну частину конкретного рішення."""
        case = decision.tracked_case.bankruptcy_case

        # Перевіряємо чи не оброблена вже ця справа
        if not force and CreditorClaim.objects.filter(case=case).exists():
            self.stdout.write(f"Справа {case.case_number} вже оброблена, пропускаємо")
            return

        # Створюємо лог аналізу
        analysis_log = LLMAnalysisLog.objects.create(
            case=case,
            analysis_type="creditor_extraction",
            status="processing",
            input_text=decision.resolution_text[:5000]  # Обмежуємо довжину для логу
        )

        start_time = time.time()

        try:
            self.stdout.write(f"Аналіз рішення {decision.doc_id} для справи {case.case_number}")

            # Викликаємо Mistral для аналізу
            analysis_result = self.mistral_service.analyze_resolutive_part(
                decision.resolution_text
            )

            processing_time = time.time() - start_time

            # Перевіряємо на помилки
            if "error" in analysis_result:
                analysis_log.status = "failed"
                analysis_log.error_message = analysis_result["error"]
                analysis_log.processing_time_seconds = processing_time
                analysis_log.completed_at = timezone.now()
                analysis_log.save()

                self.stdout.write(
                    self.style.ERROR(f"Помилка аналізу: {analysis_result["error"]}")
                )
                return

            # Обробляємо результати
            creditors_created = self.process_analysis_result(
                case, analysis_result, analysis_log
            )

            # Оновлюємо лог
            analysis_log.status = "completed"
            analysis_log.output_text = str(analysis_result)[:5000]
            analysis_log.processing_time_seconds = processing_time
            analysis_log.completed_at = timezone.now()
            analysis_log.save()

            self.stdout.write(
                self.style.SUCCESS(f"✅ Створено {creditors_created} записів кредиторів")
            )

        except Exception as e:
            processing_time = time.time() - start_time

            analysis_log.status = "failed"
            analysis_log.error_message = str(e)
            analysis_log.processing_time_seconds = processing_time
            analysis_log.completed_at = timezone.now()
            analysis_log.save()

            logger.error(f"Помилка аналізу рішення {decision.doc_id}: {e}")
            raise

    def process_analysis_result(self, case, analysis_result, analysis_log):
        """Обробляє результат аналізу та створює записи кредиторів."""
        if "creditors" not in analysis_result:
            return 0

        creditors_created = 0

        with transaction.atomic():
            for creditor_data in analysis_result["creditors"]:
                if not creditor_data.get("name"):
                    continue

                # Створюємо або знаходимо кредитора
                creditor = self.get_or_create_creditor(creditor_data["name"])

                # Витягуємо суми за чергами з нового формату
                amounts = creditor_data.get("amounts", {})

                # Створюємо або оновлюємо вимогу кредитора
                creditor_claim, created = CreditorClaim.objects.update_or_create(
                    case=case,
                    creditor=creditor,
                    defaults={
                        "amount_1st_queue": amounts.get("1st_queue", 0),
                        "amount_2nd_queue": amounts.get("2nd_queue", 0),
                        "amount_3rd_queue": amounts.get("3rd_queue", 0),
                        "amount_4th_queue": amounts.get("4th_queue", 0),
                        "amount_5th_queue": amounts.get("5th_queue", 0),
                        "amount_6th_queue": amounts.get("6th_queue", 0),
                        "llm_analysis_result": creditor_data,
                        "source_resolution_texts": str(analysis_log.id),
                        "confidence_score": analysis_result.get("confidence", 0.5)
                    }
                )

                if created:
                    creditors_created += 1
                    self.stdout.write(f"  - {creditor.name}: {creditor_claim.total_amount} грн")

        return creditors_created

    def get_or_create_creditor(self, creditor_name):
        """Створює або знаходить кредитора з нормалізацією назви."""
        normalized_name = self.normalize_creditor_name(creditor_name)

        creditor, created = Creditor.objects.get_or_create(
            name=creditor_name,
            normalized_name=normalized_name
        )

        return creditor

    def normalize_creditor_name(self, name):
        """Нормалізує назву кредитора для групування."""
        import re

        # Видаляємо організаційно-правові форми
        normalized = re.sub(
            r"\b(ТОВ|ПАТ|АТ|ПрАТ|КП|ДП|ФОП|СПД|ООО|ЗАТ|ВАТ)\b\s*",
            "",
            name,
            flags=re.IGNORECASE
        ).strip()

        # Видаляємо лапки та зайві пробіли
        normalized = re.sub(r'["\'"„""«»]', "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

