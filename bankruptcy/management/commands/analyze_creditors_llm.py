# -*- coding: utf-8 -*-
"""
Команда для аналізу справ банкрутства мовною моделлю.
Обробляє справи починаючи з найбільшого номеру та витягує дані кредиторів
з резолютивних частин, що містять тригерні слова.
"""

from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.models import BankruptcyCase, TrackedCourtDecision, LLMAnalysisLog
from bankruptcy.llm_analyzer import get_analyzer
from bankruptcy.trigger_words import has_both_triggers_in_same_sentence
import time

class Command(BaseCommand):
    help = "Аналізує справи банкрутства мовною моделлю для витягування даних кредиторів"

    def add_arguments(self, parser):
        parser.add_argument(
            "--start-case",
            type=int,
            help="Номер справи з якої почати (за замовчуванням - найбільший)"
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Кількість справ для обробки (за замовчуванням: 10)"
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

    def handle(self, *args, **options):
        start_case = options["start_case"]
        limit = options["limit"]
        min_confidence = options["min_confidence"]
        dry_run = options["dry_run"]
        force_reprocess = options["force_reprocess"]
        
        self.stdout.write(f"🧠 АНАЛІЗ СПРАВ МОВНОЮ МОДЕЛЛЮ (MISTRAL LATEST)")
        self.stdout.write(f"   - Витягування: КРЕДИТОРИ + СУМИ ПО ЧЕРГАМ")
        self.stdout.write(f"   - Ліміт справ: {limit}")
        self.stdout.write(f"   - Мін. достовірність: {min_confidence}")
        self.stdout.write(f"   - Режим: {"ТЕСТ" if dry_run else "ОБРОБКА"}")
        self.stdout.write(f"   - Повторна обробка: {"ТАК" if force_reprocess else "НІ"}")
        
        # Тестуємо з"єднання з LLM
        analyzer = get_analyzer()
        if not analyzer.test_connection():
            self.stdout.write(self.style.ERROR("❌ Не вдалося підключитися до Ollama"))
            self.stdout.write("Переконайтеся що Ollama запущений: ollama serve")
            return
        
        self.stdout.write(self.style.SUCCESS("✅ З"єднання з Ollama встановлено"))
        
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
        
        for case in cases_to_process:
            self.stdout.write(f"\n🔍 Обробка справи № {case.number} ({case.case_number})")
            
            try:
                # Отримуємо резолютивні частини з тригерами
                trigger_resolutions = self.get_trigger_resolutions(case)
                
                if not trigger_resolutions:
                    self.stdout.write("   ⚠️  Немає резолютивних частин з тригерами")
                    continue
                
                self.stdout.write(f"   📄 Знайдено {len(trigger_resolutions)} резолютивних частин з тригерами")
                
                # Обробляємо резолютивні частини групами по 1
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
                    
                    # Аналізуємо мовною моделлю
                    start_time = time.time()
                    analysis_result = analyzer.analyze_resolution_text(combined_text, case)
                    analysis_time = time.time() - start_time
                    
                    if analysis_result.get("success"):
                        creditors_found = len(analysis_result.get("creditors", []))
                        confidence = analysis_result.get("confidence", 0.0)
                        
                        self.stdout.write(f"   ✅ Група {batch_num} завершена за {analysis_time:.1f}с")
                        self.stdout.write(f"   👥 Знайдено кредиторів: {creditors_found}")
                        self.stdout.write(f"   🎯 Достовірність: {confidence:.2f}")
                        
                        # Зберігаємо тільки якщо достовірність достатня
                        if confidence >= min_confidence:
                            saved_claims = analyzer.save_creditor_claims(case, analysis_result)
                            total_saved_claims.extend(saved_claims)
                            self.stdout.write(f"   💾 Збережено вимог з групи {batch_num}: {len(saved_claims)}")
                            
                            # Показуємо збережених кредиторів з сумами
                            for claim in saved_claims:
                                total = claim.total_amount
                                amounts_str = []
                                if claim.amount_1st_queue: amounts_str.append(f"1ч: {claim.amount_1st_queue:,.0f}")
                                if claim.amount_2nd_queue: amounts_str.append(f"2ч: {claim.amount_2nd_queue:,.0f}")
                                if claim.amount_3rd_queue: amounts_str.append(f"3ч: {claim.amount_3rd_queue:,.0f}")
                                if claim.amount_4th_queue: amounts_str.append(f"4ч: {claim.amount_4th_queue:,.0f}")
                                if claim.amount_5th_queue: amounts_str.append(f"5ч: {claim.amount_5th_queue:,.0f}")
                                if claim.amount_6th_queue: amounts_str.append(f"6ч: {claim.amount_6th_queue:,.0f}")

                                amounts_details = " (" + ", ".join(amounts_str) + ")" if amounts_str else ""
                                self.stdout.write(f"      - {claim.creditor.name}: {total:,.2f} грн{amounts_details}")
                        else:
                            self.stdout.write(f"   ⚠️  Достовірність {confidence:.2f} < {min_confidence}, група {batch_num} не збережена")
                    else:
                        error_msg = analysis_result.get("error", "Невідома помилка")
                        self.stdout.write(f"   ❌ Помилка в групі {batch_num}: {error_msg}")
                        error_count += 1
                        continue
                    
                    # Очищення та пауза між групами
                    if i + batch_size < len(trigger_resolutions):
                        self.stdout.write(f"   🧹 Очищення моделі після групи {batch_num}...")
                        analyzer.unload_model()
                        self.stdout.write(f"   ⏳ Пауза 5с перед наступною групою...")
                        time.sleep(5)
                
                # Підсумкова інформація для справи
                if total_saved_claims:
                    self.stdout.write(f"   📋 Всього збережено по справі: {len(total_saved_claims)} вимог")
                    success_count += 1
                elif not dry_run:
                    self.stdout.write(f"   📭 По справі не збережено жодної вимоги")
                
                processed_count += 1
                
                # Пауза між запитами
                time.sleep(2)
                
            except Exception as e:
                self.stdout.write(f"   💥 Критична помилка: {str(e)}")
                error_count += 1
                continue
        
        # Фінальна статистика
        self.stdout.write(f"\n📊 ФІНАЛЬНА СТАТИСТИКА:")
        self.stdout.write(f"   Оброблено справ: {processed_count}")
        self.stdout.write(f"   Успішних аналізів: {success_count}")
        self.stdout.write(f"   Помилок: {error_count}")
        
        if success_count > 0:
            self.stdout.write(f"\n💡 Для перегляду результатів:")
            self.stdout.write(f"   - Сторінки справ: http://127.0.0.1:8000/case/[НОМЕР]/")
            self.stdout.write(f"   - Логи аналізу: в моделі LLMAnalysisLog")
    
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