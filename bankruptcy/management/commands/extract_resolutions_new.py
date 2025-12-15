"""
Management команда для витягування резолютивних частин з судових документів.
Базується на еталонному проекті.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from bankruptcy.models import TrackedBankruptcyCase, TrackedCourtDecision
from bankruptcy.services import ResolutionExtractionService
import time


class Command(BaseCommand):
    help = "Витягує резолютивні частини з судових документів"

    def add_arguments(self, parser):
        parser.add_argument(
            "--case-id",
            type=int,
            help="ID конкретної справи для обробки",
        )
        parser.add_argument(
            "--case-number",
            type=str,
            help="Номер справи для обробки (наприклад, 904/6740/20)",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Обробити всі справи",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Максимальна кількість документів для обробки",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Тестовий режим без збереження змін",
        )

    def handle(self, *args, **options):
        service = ResolutionExtractionService()
        
        if options["dry_run"]:
            self.stdout.write(
                self.style.WARNING("ТЕСТОВИЙ РЕЖИМ: зміни не будуть збережені")
            )
        
        # Визначаємо які справи обробляти
        if options["case_id"]:
            try:
                case = TrackedBankruptcyCase.objects.get(id=options["case_id"])
                cases = [case]
                self.stdout.write(f"Обробляємо справу ID {case.id}: {case.bankruptcy_case.case_number}")
            except TrackedBankruptcyCase.DoesNotExist:
                raise CommandError(f"Справа з ID {options["case_id"]} не знайдена")
                
        elif options["case_number"]:
            try:
                case = TrackedBankruptcyCase.objects.get(bankruptcy_case__case_number=options["case_number"])
                cases = [case]
                self.stdout.write(f"Обробляємо справу: {case.bankruptcy_case.case_number}")
            except TrackedBankruptcyCase.DoesNotExist:
                raise CommandError(f"Справа з номером {options["case_number"]} не знайдена")
                
        elif options["all"]:
            # ВАЖЛИВО: Сортуємо ВІД НОВИХ ДО СТАРИХ справ (найновіші першими)
            cases = TrackedBankruptcyCase.objects.all().order_by("bankruptcy_case__date", "created_at")
            self.stdout.write(f"Обробляємо всі {cases.count()} справ (від новіших до старіших)")
            
        else:
            raise CommandError("Вкажіть --case-id, --case-number або --all")

        # Підрахунок статистики
        total_cases = len(cases)
        total_documents = 0
        total_processed = 0
        total_success = 0
        
        self.stdout.write(f"Починаємо обробку {total_cases} справ...")
        
        for i, case in enumerate(cases, 1):
            self.stdout.write(f"\n[{i}/{total_cases}] Обробка справи: {case.bankruptcy_case.case_number}")
            
            # Знаходимо документи для обробки
            documents_query = TrackedCourtDecision.objects.filter(
                tracked_case=case,
                doc_url__isnull=False,
            ).exclude(doc_url="").exclude(doc_url="nan")
            
            # Фільтруємо документи що потребують обробки
            documents_to_process = []
            for doc in documents_query:
                if doc.needs_resolution_extraction():
                    documents_to_process.append(doc)
                    if options["limit"] and len(documents_to_process) >= options["limit"]:
                        break
            
            if not documents_to_process:
                self.stdout.write("  Немає документів для обробки")
                continue
                
            self.stdout.write(f"  Знайдено {len(documents_to_process)} документів для обробки")
            total_documents += len(documents_to_process)
            
            if options["dry_run"]:
                self.stdout.write("  ТЕСТОВИЙ РЕЖИМ: пропускаємо обробку")
                continue
            
            # Обробляємо документи
            case_processed = 0
            case_success = 0
            
            for j, document in enumerate(documents_to_process, 1):
                self.stdout.write(f"    [{j}/{len(documents_to_process)}] Обробка документа {document.doc_id}")
                
                try:
                    success = service.extractor.process_tracked_court_decision(document)
                    if success:
                        case_success += 1
                        self.stdout.write(f"      [OK] Успішно витягнута резолютивна частина")
                    else:
                        self.stdout.write(f"      [FAIL] Не вдалося витягнути резолютивну частину")
                    case_processed += 1
                    
                    # Пауза між документами щоб не перевантажувати сервер
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.stdout.write(f"      [ERROR] Помилка: {str(e)}")
                    
            total_processed += case_processed
            total_success += case_success
            
            self.stdout.write(f"  Справа завершена: {case_success}/{case_processed} успішно")
            
            # Пауза між справами
            time.sleep(1)
        
        # Фінальна статистика
        self.stdout.write(f"\n{"-"*50}")
        self.stdout.write(f"СТАТИСТИКА ОБРОБКИ:")
        self.stdout.write(f"  Справ оброблено: {total_cases}")
        self.stdout.write(f"  Документів знайдено: {total_documents}")
        if not options["dry_run"]:
            self.stdout.write(f"  Документів оброблено: {total_processed}")
            self.stdout.write(f"  Успішно витягнуто: {total_success}")
            success_rate = (total_success / total_processed * 100) if total_processed > 0 else 0
            self.stdout.write(f"  Показник успіху: {success_rate:.1f}%")
        
        self.stdout.write(
            self.style.SUCCESS(f"\nОбробка завершена!")
        )