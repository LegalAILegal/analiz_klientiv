# -*- coding: utf-8 -*-
"""
Команда для аналізу справ банкрутства за допомогою Anthropic Claude API v2.
Використовує офіційну бібліотеку anthropic.
"""

from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.models import BankruptcyCase, TrackedCourtDecision, LLMAnalysisLog
from bankruptcy.anthropic_analyzer_v2 import get_anthropic_analyzer_v2
from bankruptcy.trigger_words import has_both_triggers_in_same_sentence
import time

class Command(BaseCommand):
    help = "Аналізує справи банкрутства за допомогою Anthropic Claude API v2"

    def add_arguments(self, parser):
        parser.add_argument(
            "--start-case",
            type=int,
            help="Номер справи з якої почати (за замовчуванням - найбільший)"
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=5,
            help="Кількість справ для обробки (за замовчуванням: 5)"
        )
        parser.add_argument(
            "--min-confidence",
            type=float,
            default=0.7,
            help="Мінімальна оцінка достовірності для збереження (за замовчуванням: 0.7)"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Тільки показати що буде оброблено, не зберігати результати"
        )
        parser.add_argument(
            "--force-reprocess",
            action="store_true",
            help="Повторно обробити справи, які вже мають аналіз"
        )
        parser.add_argument(
            "--rate-limit",
            type=float,
            default=3.0,
            help="Затримка між запитами в секундах (за замовчуванням: 3.0)"
        )

    def handle(self, *args, **options):
        start_case = options["start_case"]
        limit = options["limit"]
        min_confidence = options["min_confidence"]
        dry_run = options["dry_run"]
        force_reprocess = options["force_reprocess"]
        rate_limit = options["rate_limit"]

        self.stdout.write(f"🤖 АНАЛІЗ СПРАВ ANTHROPIC CLAUDE API V2")
        self.stdout.write(f"   - Модель: claude-sonnet-4-20250514")
        self.stdout.write(f"   - Бібліотека: anthropic (офіційна)")
        self.stdout.write(f"   - Ліміт справ: {limit}")
        self.stdout.write(f"   - Мін. достовірність: {min_confidence}")
        self.stdout.write(f"   - Режим: {"ТЕСТ" if dry_run else "ОБРОБКА"}")
        self.stdout.write(f"   - Повторна обробка: {"ТАК" if force_reprocess else "НІ"}")
        self.stdout.write(f"   - Контроль швидкості: {rate_limit}с між запитами")

        # Тестуємо з"єднання з Anthropic
        analyzer = get_anthropic_analyzer_v2()
        analyzer.rate_limit_delay = rate_limit  # Встановлюємо затримку

        if not analyzer.test_connection():
            self.stdout.write(self.style.ERROR("❌ Не вдалося підключитися до Anthropic API v2"))
            self.stdout.write("Перевірте API ключ та інтернет з"єднання")
            return

        self.stdout.write(self.style.SUCCESS("✅ З"єднання з Anthropic API v2 встановлено"))

        # Отримуємо справи для обробки
        cases_to_process = self.get_cases_to_process(start_case, limit, force_reprocess)

        if not cases_to_process:
            self.stdout.write("📭 Немає справ для обробки")
            return

        self.stdout.write(f"📋 Знайдено {len(cases_to_process)} справ для обробки")

        # Обробляємо кожну справу
        processed_count = 0
        success_count = 0
        error_count = 0
        total_requests = 0

        for case in cases_to_process:
            self.stdout.write(f"\n🔍 Обробка справи № {case.number} ({case.case_number})")

            try:
                # Отримуємо резолютивні частини з тригерами
                trigger_resolutions = self.get_trigger_resolutions(case)

                if not trigger_resolutions:
                    self.stdout.write("   ⚠️  Немає резолютивних частин з тригерами")
                    continue

                self.stdout.write(f"   📄 Знайдено {len(trigger_resolutions)} резолютивних частин з тригерами")

                # Обробляємо резолютивні частини групами по 1 для контролю швидкості
                batch_size = 1
                total_saved_claims = []

                for i in range(0, len(trigger_resolutions), batch_size):
                    batch = trigger_resolutions[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    total_batches = (len(trigger_resolutions) + batch_size - 1) // batch_size

                    self.stdout.write(f"   📦 Обробка групи {batch_num}/{total_batches} ({len(batch)} частин)")

                    # Об"єднуємо резолютивні частини в групі
                    combined_text = "\n\n---\n\n".join([
                        decision["resolution_text"] for decision in batch
                    ])

                    if dry_run:
                        self.stdout.write(f"   🧪 ТЕСТ: Буде проаналізовано {len(combined_text)} символів")
                        continue

                    # Аналізуємо за допомогою Claude API з контролем швидкості
                    start_time = time.time()
                    self.stdout.write(f"   ⏳ Відправка запиту до Claude API v2...")

                    analysis_result = analyzer.analyze_resolution_text(combined_text, case)
                    total_requests += 1

                    analysis_time = time.time() - start_time

                    if analysis_result.get("success"):
                        creditors_found = len(analysis_result.get("creditors", []))
                        confidence = analysis_result.get("confidence", 0.0)
                        usage = analysis_result.get("usage", {})

                        self.stdout.write(f"   ✅ Група {batch_num} завершена за {analysis_time:.1f}с")
                        self.stdout.write(f"   👥 Знайдено кредиторів: {creditors_found}")
                        self.stdout.write(f"   🎯 Достовірність: {confidence:.2f}")

                        if usage:
                            input_tokens = usage.get("input_tokens", 0)
                            output_tokens = usage.get("output_tokens", 0)
                            self.stdout.write(f"   📊 Токени: вхід={input_tokens}, вихід={output_tokens}")

                        # Зберігаємо тільки якщо достовірність достатня
                        if confidence >= min_confidence:
                            saved_claims = analyzer.save_creditor_claims(case, analysis_result)
                            total_saved_claims.extend(saved_claims)
                            self.stdout.write(f"   💾 Збережено вимог з групи {batch_num}: {len(saved_claims)}")

                            # Показуємо збережених кредиторів
                            for claim in saved_claims:
                                total = claim.total_amount
                                self.stdout.write(f"      - {claim.creditor.name}: {total:,.2f} грн")
                        else:
                            self.stdout.write(f"   ⚠️  Достовірність {confidence:.2f} < {min_confidence}, група {batch_num} не збережена")
                    else:
                        error_msg = analysis_result.get("error", "Невідома помилка")
                        self.stdout.write(f"   ❌ Помилка в групі {batch_num}: {error_msg}")
                        error_count += 1
                        continue

                    # Контроль швидкості - затримка між запитами
                    if i + batch_size < len(trigger_resolutions):
                        self.stdout.write(f"   ⏳ Затримка {rate_limit}с перед наступним запитом...")
                        time.sleep(rate_limit)

                # Підсумкова інформація для справи
                if total_saved_claims:
                    self.stdout.write(f"   📋 Всього збережено по справі: {len(total_saved_claims)} вимог")
                    success_count += 1
                elif not dry_run:
                    self.stdout.write(f"   📭 По справі не збережено жодної вимоги")

                processed_count += 1

                # Затримка між справами
                if processed_count < len(cases_to_process):
                    self.stdout.write(f"   ⏳ Пауза {rate_limit * 2}с перед наступною справою...")
                    time.sleep(rate_limit * 2)

            except Exception as e:
                self.stdout.write(f"   💥 Критична помилка: {str(e)}")
                error_count += 1
                continue

        # Фінальна статистика
        self.stdout.write(f"\n📊 ФІНАЛЬНА СТАТИСТИКА:")
        self.stdout.write(f"   Оброблено справ: {processed_count}")
        self.stdout.write(f"   Успішних аналізів: {success_count}")
        self.stdout.write(f"   Помилок: {error_count}")
        self.stdout.write(f"   API запитів: {total_requests}")

        if success_count > 0:
            self.stdout.write(f"\n💡 Для перегляду результатів:")
            self.stdout.write(f"   - Сторінки справ: http://127.0.0.1:8000/case/[НОМЕР]/")
            self.stdout.write(f"   - Статистика кредиторів: http://127.0.0.1:8000/creditors/")

    def get_cases_to_process(self, start_case, limit, force_reprocess):
        """Отримує справи для обробки"""

        # Базовий запит
        queryset = BankruptcyCase.objects.all()

        if not force_reprocess:
            # Виключаємо справи, які вже мають аналіз
            analyzed_case_ids = LLMAnalysisLog.objects.filter(
                analysis_type="creditor_extraction",
                status="completed"
            ).values_list("case_id", flat=True).distinct()

            queryset = queryset.exclude(id__in=analyzed_case_ids)

        # Фільтр за номером справи
        if start_case:
            queryset = queryset.filter(number__lte=start_case)

        # Сортуємо за спаданням номера (найбільші спочатку)
        queryset = queryset.order_by("-number")

        # Обмежуємо кількість
        return list(queryset[:limit])

    def get_trigger_resolutions(self, case):
        """Отримує резолютивні частини з тригерними словами для справи"""

        # Знаходимо відстежувані рішення для цієї справи
        tracked_decisions = TrackedCourtDecision.objects.filter(
            tracked_case__bankruptcy_case=case,
            resolution_text__isnull=False
        ).exclude(resolution_text="").exclude(resolution_text="-")

        trigger_resolutions = []

        for decision in tracked_decisions:
            if has_both_triggers_in_same_sentence(decision.resolution_text):
                trigger_resolutions.append({
                    "doc_id": decision.doc_id,
                    "resolution_text": decision.resolution_text,
                    "court_name": decision.court_name,
                    "adjudication_date": decision.adjudication_date
                })

        return trigger_resolutions