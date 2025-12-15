from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
import time

from bankruptcy.models import TrackedBankruptcyCase
from bankruptcy.utils.fast_court_search import FastCourtSearch
from bankruptcy.services import BankruptcyCaseSearchService


class Command(BaseCommand):
    help = "Тестує швидкий точний пошук судових рішень (адаптація SR_AI)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--case-number",
            type=str,
            help="Номер конкретної справи для тестування",
        )
        parser.add_argument(
            "--test-variants",
            action="store_true",
            help="Тестувати генерацію варіантів номерів справ",
        )
        parser.add_argument(
            "--compare-methods",
            action="store_true",
            help="Порівняти швидкий і стандартний методи пошуку",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=5,
            help="Кількість справ для тестування (за замовчуванням: 5)",
        )

    def handle(self, *args, **options):
        case_number = options["case_number"]
        test_variants = options["test_variants"]
        compare_methods = options["compare_methods"]
        limit = options["limit"]

        self.stdout.write("🧪 ТЕСТУВАННЯ ШВИДКОГО ПОШУКУ СУДОВИХ РІШЕНЬ")
        self.stdout.write("📋 Базується на принципах SR_AI з адаптацією для точного пошуку\n")

        if test_variants:
            self._test_case_variants()

        if case_number:
            self._test_specific_case(case_number)

        if compare_methods:
            self._compare_search_methods(limit)

    def _test_case_variants(self):
        """Тестує генерацію варіантів номерів справ"""
        self.stdout.write("🔍 ТЕСТ: Генерація варіантів номерів справ\n")
        
        fast_search = FastCourtSearch()
        
        test_cases = [
            "756/16936/23",
            "904/1234/2022",
            "123/456/99",
            "999/888/01",
        ]
        
        for case_num in test_cases:
            variants = fast_search.generate_exact_case_variants(case_num)
            self.stdout.write(f"📋 {case_num} → {variants}")
        
        self.stdout.write("")

    def _test_specific_case(self, case_number):
        """Тестує пошук для конкретної справи"""
        self.stdout.write(f"🔍 ТЕСТ: Пошук для справи {case_number}\n")
        
        try:
            # Знаходимо справу в базі
            tracked_case = TrackedBankruptcyCase.objects.filter(
                bankruptcy_case__case_number__icontains=case_number
            ).first()
            
            if not tracked_case:
                self.stdout.write(
                    self.style.WARNING(f"⚠️ Справа з номером {case_number} не знайдена в базі")
                )
                return
            
            self.stdout.write(f"✅ Знайдено справу: {tracked_case.bankruptcy_case.case_number}")
            
            # Виконуємо швидкий пошук
            fast_search = FastCourtSearch()
            
            start_time = time.time()
            found_decisions = fast_search.search_single_case_exact(tracked_case)
            end_time = time.time()
            
            search_time = end_time - start_time
            
            self.stdout.write(f"⏱️ Час пошуку: {search_time:.3f} секунд")
            self.stdout.write(f"📊 Знайдено рішень: {len(found_decisions)}")
            
            if found_decisions:
                self.stdout.write("\n📋 Знайдені рішення:")
                for i, decision in enumerate(found_decisions[:3], 1):  # Показуємо перші 3
                    self.stdout.write(f"  {i}. Doc ID: {decision.doc_id}")
                    self.stdout.write(f"     Номер справи: {decision.cause_num}")
                    self.stdout.write(f"     Суд: {decision.court_code}")
                    self.stdout.write(f"     Джерело: {decision.source_info}")
                
                if len(found_decisions) > 3:
                    self.stdout.write(f"     ... та ще {len(found_decisions) - 3} рішень")
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Помилка тестування: {e}")
            )
        
        self.stdout.write("")

    def _compare_search_methods(self, limit):
        """Порівнює швидкий і стандартний методи пошуку"""
        self.stdout.write(f"⚖️ ПОРІВНЯННЯ МЕТОДІВ ПОШУКУ (перші {limit} справ)\n")
        
        # Отримуємо справи для тестування
        test_cases = TrackedBankruptcyCase.objects.filter(
            bankruptcy_case__case_number__isnull=False
        ).exclude(
            bankruptcy_case__case_number__exact=""
        ).exclude(
            bankruptcy_case__case_number__exact="nan"
        )[:limit]
        
        if not test_cases:
            self.stdout.write("⚠️ Немає справ для тестування")
            return
        
        self.stdout.write(f"📋 Тестування {len(test_cases)} справ...\n")
        
        fast_search = FastCourtSearch()
        standard_service = BankruptcyCaseSearchService()
        # Вимикаємо швидкий пошук для стандартного сервісу
        standard_service.use_fast_search = False
        
        results = {
            "fast_search": {"time": 0, "decisions": 0, "cases": 0},
            "standard_search": {"time": 0, "decisions": 0, "cases": 0}
        }
        
        # Тестуємо швидкий пошук
        self.stdout.write("🚀 Тестування швидкого пошуку...")
        start_time = time.time()
        
        for case in test_cases:
            try:
                decisions = fast_search.search_single_case_exact(case)
                results["fast_search"]["decisions"] += len(decisions)
                results["fast_search"]["cases"] += 1
            except Exception as e:
                self.stdout.write(f"  ⚠️ Помилка швидкого пошуку для {case.bankruptcy_case.case_number}: {e}")
        
        results["fast_search"]["time"] = time.time() - start_time
        
        # Тестуємо стандартний пошук
        self.stdout.write("🐌 Тестування стандартного пошуку...")
        start_time = time.time()
        
        for case in test_cases:
            try:
                decisions_count = standard_service.search_and_save_court_decisions(case)
                results["standard_search"]["decisions"] += decisions_count
                results["standard_search"]["cases"] += 1
            except Exception as e:
                self.stdout.write(f"  ⚠️ Помилка стандартного пошуку для {case.bankruptcy_case.case_number}: {e}")
        
        results["standard_search"]["time"] = time.time() - start_time
        
        # Виводимо результати порівняння
        self.stdout.write("\n" + "="*60)
        self.stdout.write("📊 РЕЗУЛЬТАТИ ПОРІВНЯННЯ:")
        self.stdout.write("="*60)
        
        fast_time = results["fast_search"]["time"]
        std_time = results["standard_search"]["time"]
        
        self.stdout.write(f"🚀 Швидкий пошук:")
        self.stdout.write(f"   ⏱️ Час: {fast_time:.3f} секунд")
        self.stdout.write(f"   📋 Справ оброблено: {results["fast_search"]["cases"]}")
        self.stdout.write(f"   📊 Знайдено рішень: {results["fast_search"]["decisions"]}")
        
        self.stdout.write(f"\n🐌 Стандартний пошук:")
        self.stdout.write(f"   ⏱️ Час: {std_time:.3f} секунд") 
        self.stdout.write(f"   📋 Справ оброблено: {results["standard_search"]["cases"]}")
        self.stdout.write(f"   📊 Знайдено рішень: {results["standard_search"]["decisions"]}")
        
        if fast_time > 0 and std_time > 0:
            speedup = std_time / fast_time
            self.stdout.write(f"\n🎯 ПРИСКОРЕННЯ: {speedup:.2f}x")
            
            if speedup > 5:
                self.stdout.write("🏆 Відмінний результат! SR_AI принципи працюють!")
            elif speedup > 2:
                self.stdout.write("✅ Хороше прискорення!")
            else:
                self.stdout.write("⚠️ Потрібна додаткова оптимізація")
        
        self.stdout.write("="*60)