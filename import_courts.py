#!/usr/bin/env python
"""Скрипт для імпорту даних про суди"""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analiz_klientiv.settings')
django.setup()

import csv
from django.db import connection

print("Імпорт даних про суди з courts.csv...")

# Очищуємо таблицю перед імпортом
with connection.cursor() as cursor:
    cursor.execute("DELETE FROM bankruptcy_courts_ref")
    print(f"Очищено таблицю bankruptcy_courts_ref")

imported = 0
with open('/app/data/courts.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')

    with connection.cursor() as cursor:
        for row in reader:
            court_code = row['court_code'].strip()
            name = row['name'].strip().strip('"')
            instance_code = row.get('instance_code', '').strip() or None
            region_code = row.get('region_code', '').strip() or None

            cursor.execute("""
                INSERT INTO bankruptcy_courts_ref (court_code, name, instance_code, region_code)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (court_code) DO UPDATE
                SET name = EXCLUDED.name,
                    instance_code = EXCLUDED.instance_code,
                    region_code = EXCLUDED.region_code
            """, [court_code, name, instance_code, region_code])

            imported += 1
            if imported % 100 == 0:
                print(f"  Імпортовано {imported} судів...")

print(f"\n{'='*60}")
print(f"ПІДСУМОК: Імпортовано {imported} судів")

# Статистика
with connection.cursor() as cursor:
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(instance_code) as with_instance,
            COUNT(DISTINCT instance_code) as unique_instances
        FROM bankruptcy_courts_ref
    """)
    row = cursor.fetchone()
    print(f"Всього судів: {row[0]}")
    print(f"З кодом інстанції: {row[1]}")
    print(f"Унікальних інстанцій: {row[2]}")

    # Розподіл по інстанціях
    cursor.execute("""
        SELECT instance_code, COUNT(*)
        FROM bankruptcy_courts_ref
        WHERE instance_code IS NOT NULL
        GROUP BY instance_code
        ORDER BY instance_code
    """)
    print("\nРозподіл по інстанціях:")
    for inst_code, count in cursor.fetchall():
        print(f"  Інстанція {inst_code}: {count} судів")

print('='*60)
