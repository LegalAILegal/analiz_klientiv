#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import django
from django.db import transaction

# Налаштовуємо Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analiz_klientiv.settings')
django.setup()

from bankruptcy.models import Creditor, CreditorClaim

def normalize_quotes(text):
    """Стандартизує лапки в тексті"""
    if not text:
        return text

    result = text
    result = result.replace("'", '"')  # одинарні
    result = result.replace("'", '"')  # ліва одинарна
    result = result.replace("'", '"')  # права одинарна
    result = result.replace(""", '"')  # ліва подвійна
    result = result.replace(""", '"')  # права подвійна
    result = result.replace("„", '"')  # нижня подвійна
    result = result.replace("«", '"')  # французькі ліві
    result = result.replace("»", '"')  # французькі праві

    return result

def fix_creditor_quotes():
    """Стандартизує лапки в назвах кредиторів з об'єднанням дублікатів"""

    print("Починаємо розумну стандартизацію лапок у назвах кредиторів...")

    all_creditors = Creditor.objects.all()
    print(f"Загалом кредиторів у базі: {all_creditors.count()}")

    updated_count = 0
    merged_count = 0

    with transaction.atomic():
        for creditor in all_creditors:
            old_name = creditor.name
            new_name = normalize_quotes(old_name)

            if old_name != new_name:
                print(f"Обробляємо: {old_name} -> {new_name}")

                # Перевіряємо, чи існує кредитор з новою назвою
                existing_creditor = Creditor.objects.filter(name=new_name).exclude(id=creditor.id).first()

                if existing_creditor:
                    print(f"  Знайдено дублікат. Об'єднуємо з існуючим кредитором (ID: {existing_creditor.id})")

                    # Переносимо всі вимоги від старого кредитора до існуючого
                    claims_to_move = CreditorClaim.objects.filter(creditor=creditor)
                    moved_claims = 0

                    for claim in claims_to_move:
                        # Перевіряємо, чи існує вже така вимога
                        existing_claim = CreditorClaim.objects.filter(
                            case=claim.case,
                            creditor=existing_creditor
                        ).first()

                        if existing_claim:
                            # Об'єднуємо суми
                            existing_claim.amount_1st_queue = (existing_claim.amount_1st_queue or 0) + (claim.amount_1st_queue or 0)
                            existing_claim.amount_2nd_queue = (existing_claim.amount_2nd_queue or 0) + (claim.amount_2nd_queue or 0)
                            existing_claim.amount_3rd_queue = (existing_claim.amount_3rd_queue or 0) + (claim.amount_3rd_queue or 0)
                            existing_claim.amount_4th_queue = (existing_claim.amount_4th_queue or 0) + (claim.amount_4th_queue or 0)
                            existing_claim.amount_5th_queue = (existing_claim.amount_5th_queue or 0) + (claim.amount_5th_queue or 0)
                            existing_claim.amount_6th_queue = (existing_claim.amount_6th_queue or 0) + (claim.amount_6th_queue or 0)
                            existing_claim.save()
                            claim.delete()
                            print(f"    Об'єднано вимогу для справи {claim.case.case_number}")
                        else:
                            # Переносимо вимогу
                            claim.creditor = existing_creditor
                            claim.save()
                            print(f"    Перенесено вимогу для справи {claim.case.case_number}")

                        moved_claims += 1

                    # Оновлюємо статистику існуючого кредитора
                    existing_creditor.update_statistics()

                    # Видаляємо старого кредитора
                    creditor.delete()
                    print(f"    Видалено дублікат. Перенесено {moved_claims} вимог")
                    merged_count += 1

                else:
                    # Просто оновлюємо назву
                    creditor.name = new_name
                    creditor.save()
                    print(f"    Оновлено назву")
                    updated_count += 1

    print(f"\nЗавершено!")
    print(f"Оновлено назв: {updated_count}")
    print(f"Об'єднано дублікатів: {merged_count}")

    # Перерахуємо загальну кількість
    final_count = Creditor.objects.count()
    print(f"Підсумкова кількість кредиторів: {final_count}")

    return updated_count, merged_count

if __name__ == "__main__":
    fix_creditor_quotes()