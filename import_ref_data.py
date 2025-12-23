#!/usr/bin/env python
"""Скрипт для імпорту довідкових даних"""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analiz_klientiv.settings')
django.setup()

import csv
from bankruptcy.models import Instance, JusticeKind, JudgmentForm

# Імпорт інстанцій
print("Імпорт інстанцій...")
with open('/app/data/instances.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        obj, created = Instance.objects.get_or_create(
            code=int(row['instance_code']),
            defaults={'name': row['name']}
        )
        if created:
            print(f"  Створено: {obj.name} (код {obj.code})")

# Імпорт видів судочинства
print("\nІмпорт видів судочинства...")
with open('/app/data/justice_kinds.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        obj, created = JusticeKind.objects.get_or_create(
            code=int(row['justice_kind']),
            defaults={'name': row['name']}
        )
        if created:
            print(f"  Створено: {obj.name} (код {obj.code})")

# Імпорт форм рішень
print("\nІмпорт форм рішень...")
with open('/app/data/judgment_forms.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        # Видалення лапок з назви
        name = row['name'].strip('"')
        obj, created = JudgmentForm.objects.get_or_create(
            code=int(row['judgment_code']),
            defaults={'name': name}
        )
        if created:
            print(f"  Створено: {obj.name} (код {obj.code})")

# Підсумок
print("\n" + "="*50)
print("ПІДСУМОК:")
print(f"Інстанції: {Instance.objects.count()}")
print(f"Види судочинства: {JusticeKind.objects.count()}")
print(f"Форми рішень: {JudgmentForm.objects.count()}")
print("="*50)
