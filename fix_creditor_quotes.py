#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import django
import re

# Налаштовуємо Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analiz_klientiv.settings')
django.setup()

from bankruptcy.models import Creditor

def fix_creditor_quotes():
    """Стандартизує лапки в назвах кредиторів"""

    print("Починаємо стандартизацію лапок у назвах кредиторів...")

    # Знаходимо всіх кредиторів
    all_creditors = Creditor.objects.all()
    print(f"Загалом кредиторів у базі: {all_creditors.count()}")

    updated_count = 0

    for creditor in all_creditors:
        old_name = creditor.name

        # Заміняємо всі варіанти лапок на стандартні подвійні лапки
        new_name = old_name
        new_name = new_name.replace("'", '"')  # одинарні
        new_name = new_name.replace("'", '"')  # ліва одинарна
        new_name = new_name.replace("'", '"')  # права одинарна
        new_name = new_name.replace(""", '"')  # ліва подвійна
        new_name = new_name.replace(""", '"')  # права подвійна
        new_name = new_name.replace("„", '"')  # нижня подвійна
        new_name = new_name.replace("«", '"')  # французькі ліві
        new_name = new_name.replace("»", '"')  # французькі праві

        if old_name != new_name:
            print(f"Оновлюємо: {old_name} -> {new_name}")
            creditor.name = new_name
            creditor.save()
            updated_count += 1

    print(f"\nЗавершено! Оновлено {updated_count} кредиторів")
    return updated_count

if __name__ == "__main__":
    fix_creditor_quotes()