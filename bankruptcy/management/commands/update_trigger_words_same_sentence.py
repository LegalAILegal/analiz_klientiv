# -*- coding: utf-8 -*-
"""
Команда для оновлення тригерних слів в базі даних згідно з новою логікою:
Тільки резолютивні частини з "визнати" та "грошові вимоги" в ОДНОМУ реченні
позначаються як такі, що містять тригерні слова.
"""

from django.core.management.base import BaseCommand
from django.db import connection
from bankruptcy.trigger_words import has_both_triggers_in_same_sentence
import time

class Command(BaseCommand):
    help = "Оновлює has_trigger_words для резолютивних частин згідно з новою логікою одного речення"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Розмір батчу для обробки (за замовчуванням: 1000)"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Тільки показати статистику, не оновлювати базу даних"
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        dry_run = options["dry_run"]
        
        self.stdout.write(f"🎯 ОНОВЛЕННЯ ТРИГЕРНИХ СЛІВ З НОВОЮ ЛОГІКОЮ")
        self.stdout.write(f"   - Умова: "визнати" + "грошові вимоги" в ОДНОМУ реченні")
        self.stdout.write(f"   - Розмір батчу: {batch_size}")
        self.stdout.write(f"   - Режим: {"ТЕСТ" if dry_run else "ОНОВЛЕННЯ"}")
        
        cursor = connection.cursor()
        
        # Отримуємо список таблиць судових рішень
        cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'court_decisions_%" AND table_schema = 'public'
        ORDER BY table_name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        total_processed = 0
        total_with_triggers_old = 0
        total_with_triggers_new = 0
        total_changed = 0
        
        for table in tables:
            self.stdout.write(f"\n📊 Обробка таблиці: {table}")
            
            # Підрахунок загальної кількості записів з резолютивними частинами
            cursor.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE resolution_text IS NOT NULL AND resolution_text != ''
            """)
            total_in_table = cursor.fetchone()[0]
            
            # Підрахунок поточних записів з тригерами
            cursor.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE has_trigger_words = true
            """)
            current_triggers = cursor.fetchone()[0]
            
            self.stdout.write(f"   Всього записів з резолютивними частинами: {total_in_table}")
            self.stdout.write(f"   Поточних записів з тригерами: {current_triggers}")
            
            table_processed = 0
            table_with_triggers_new = 0
            table_changed = 0
            
            # Обробка батчами
            offset = 0
            while True:
                cursor.execute(f"""
                SELECT id, resolution_text, has_trigger_words 
                FROM {table}
                WHERE resolution_text IS NOT NULL AND resolution_text != ''
                ORDER BY id
                LIMIT %s OFFSET %s
                """, [batch_size, offset])
                
                batch = cursor.fetchall()
                if not batch:
                    break
                
                batch_updates = []
                
                for record_id, resolution_text, current_has_triggers in batch:
                    # Перевіряємо нову логіку
                    new_has_triggers = has_both_triggers_in_same_sentence(resolution_text)
                    
                    if new_has_triggers != current_has_triggers:
                        batch_updates.append((record_id, new_has_triggers))
                        table_changed += 1
                    
                    if new_has_triggers:
                        table_with_triggers_new += 1
                
                # Оновлюємо базу даних якщо не dry-run
                if not dry_run and batch_updates:
                    for record_id, new_has_triggers in batch_updates:
                        cursor.execute(f"""
                        UPDATE {table} 
                        SET has_trigger_words = %s 
                        WHERE id = %s
                        """, [new_has_triggers, record_id])
                
                table_processed += len(batch)
                offset += batch_size
                
                # Прогрес
                if table_processed % (batch_size * 10) == 0:
                    self.stdout.write(f"   Оброблено: {table_processed}/{total_in_table}")
            
            total_processed += table_processed
            total_with_triggers_old += current_triggers
            total_with_triggers_new += table_with_triggers_new
            total_changed += table_changed
            
            self.stdout.write(f"   ✅ Завершено: {table_processed} записів")
            self.stdout.write(f"   📊 З тригерами (нова логіка): {table_with_triggers_new}")
            self.stdout.write(f"   🔄 Змінено записів: {table_changed}")
        
        # Фінальна статистика
        self.stdout.write(f"\n🎯 ФІНАЛЬНА СТАТИСТИКА:")
        self.stdout.write(f"   Всього оброблено записів: {total_processed}")
        self.stdout.write(f"   З тригерами (стара логіка): {total_with_triggers_old}")
        self.stdout.write(f"   З тригерами (нова логіка): {total_with_triggers_new}")
        self.stdout.write(f"   Змінено записів: {total_changed}")
        self.stdout.write(f"   Різниця: {total_with_triggers_new - total_with_triggers_old:+d}")
        
        if dry_run:
            self.stdout.write(f"\n⚠️  ТЕСТОВИЙ РЕЖИМ - зміни НЕ збережено!")
            self.stdout.write(f"   Запустіть без --dry-run для збереження змін")
        else:
            # Commit змін
            connection.commit()
            self.stdout.write(f"\n✅ ОНОВЛЕННЯ ЗАВЕРШЕНО УСПІШНО!")