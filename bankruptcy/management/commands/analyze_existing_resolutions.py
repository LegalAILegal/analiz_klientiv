from django.core.management.base import BaseCommand
from bankruptcy.models import TrackedCourtDecision
from bankruptcy.trigger_words import has_trigger_words


class Command(BaseCommand):
    help = "Аналізує тригерні слова в існуючих резолютивних частинах"

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=1000,
            help="Максимальна кількість рішень для обробки"
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Розмір батчу для обробки"
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        batch_size = options["batch_size"]
        
        # Знаходимо рішення з резолютивними частинами, але без аналізу тригерів
        decisions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False,
            has_trigger_words=False  # Ще не аналізовані
        ).exclude(
            resolution_text__exact=""
        ).exclude(
            resolution_text__icontains="Помилка"
        ).exclude(
            resolution_text__icontains="не знайдена"
        ).exclude(
            resolution_text__icontains="Не вдалося"
        )[:limit]
        
        total_count = decisions.count()
        
        if total_count == 0:
            self.stdout.write(
                self.style.WARNING("Немає рішень для аналізу тригерів")
            )
            return
        
        self.stdout.write(
            self.style.SUCCESS(f"Знайдено {total_count} рішень для аналізу тригерів")
        )
        
        processed = 0
        found_triggers = 0
        critical_decisions = 0
        
        # Обробляємо батчами
        for i in range(0, total_count, batch_size):
            batch = decisions[i:i+batch_size]
            
            for decision in batch:
                try:
                    analysis = has_trigger_words(decision.resolution_text)
                    
                    # Оновлюємо поля на основі аналізу
                    decision.has_trigger_words = analysis["has_triggers"]
                    decision.trigger_words_found = analysis["found_triggers"]
                    decision.trigger_types = analysis["trigger_types"]
                    decision.is_critical_decision = analysis["is_critical"]
                    
                    decision.save(update_fields=[
                        "has_trigger_words", 
                        "trigger_words_found", 
                        "trigger_types", 
                        "is_critical_decision"
                    ])
                    
                    processed += 1
                    
                    if analysis["has_triggers"]:
                        found_triggers += 1
                        
                    if analysis["is_critical"]:
                        critical_decisions += 1
                        
                        # Показуємо критичні рішення
                        self.stdout.write(
                            self.style.WARNING(
                                f"КРИТИЧНЕ рішення {decision.id}: {analysis["found_triggers"]}"
                            )
                        )
                        
                    # Показуємо прогрес
                    if processed % batch_size == 0:
                        self.stdout.write(
                            f"Оброблено: {processed}/{total_count}, "
                            f"з тригерами: {found_triggers}, "
                            f"критичних: {critical_decisions}"
                        )
                        
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"Помилка при обробці рішення {decision.id}: {e}")
                    )
        
        # Підсумок
        self.stdout.write(
            self.style.SUCCESS(
                f"\n=== РЕЗУЛЬТАТИ АНАЛІЗУ ===\n"
                f"Оброблено рішень: {processed}\n"
                f"Знайдено з тригерами: {found_triggers} ({found_triggers/processed*100:.1f}%)\n"
                f"Критичних рішень: {critical_decisions} ({critical_decisions/processed*100:.1f}%)"
            )
        )
        
        # Статистика по типам тригерів
        resolution_triggers = TrackedCourtDecision.objects.filter(
            trigger_types__contains="resolution"
        ).count()
        
        judgment_type_triggers = TrackedCourtDecision.objects.filter(
            trigger_types__contains="judgment_type"
        ).count()
        
        critical_triggers = TrackedCourtDecision.objects.filter(
            trigger_types__contains="critical"
        ).count()
        
        self.stdout.write(
            f"\n=== СТАТИСТИКА ПО ТИПАХ ТРИГЕРІВ ===\n"
            f"Резолютивні тригери: {resolution_triggers}\n"
            f"Тригери типів рішень: {judgment_type_triggers}\n"
            f"Критичні тригери: {critical_triggers}"
        )