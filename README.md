# Аналіз клієнтів - Система аналізу справ про банкрутство

Система для обробки та аналізу даних про провадження у справах про банкрутство в Україні.

## Налаштування проєкту

### 1. Встановлення залежностей

Для роботи проєкту потрібно встановити залежності Python:

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-pip python3-venv postgresql postgresql-contrib

# Створення віртуального середовища (рекомендовано)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# або
venv\Scripts\activate  # Windows

# Встановлення залежностей
pip install -r requirements.txt
```

### 2. Налаштування PostgreSQL

Встановіть та налаштуйте PostgreSQL:

```bash
# Встановлення PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Створення бази даних та користувача
sudo -u postgres psql -c "CREATE DATABASE analiz_klientiv;"
sudo -u postgres psql -c "CREATE USER analiz_user WITH PASSWORD 'analiz_password';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE analiz_klientiv TO analiz_user;"
sudo -u postgres psql -d analiz_klientiv -c "GRANT ALL ON SCHEMA public TO analiz_user;"
sudo -u postgres psql -d analiz_klientiv -c "ALTER USER analiz_user CREATEDB;"
```

### 3. Налаштування Django

Відредагуйте файл `analiz_klientiv/settings.py` з правильними налаштуваннями бази даних:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'analiz_klientiv',
        'USER': 'analiz_user',
        'PASSWORD': 'analiz_password',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

### 4. Створення міграцій та налаштування бази

```bash
# Створення міграцій
python manage.py makemigrations bankruptcy

# Застосування міграцій
python manage.py migrate

# Створення суперкористувача
python manage.py createsuperuser
```

### 5. Завантаження даних

Завантажте дані з CSV файлу:

```bash
# Повне завантаження (видаляє попередні дані)
python manage.py load_bankruptcy_data

# Інкрементальне завантаження (тільки нові записи)
python manage.py load_bankruptcy_data --incremental

# Вказати інший файл
python manage.py load_bankruptcy_data --file /path/to/your/file.csv
```

### 6. Запуск сервера

```bash
python manage.py runserver
```

Відкрийте http://127.0.0.1:8000 у браузері.

## Структура проєкту

```
analiz_klientiv/
├── manage.py                          # Django management script
├── requirements.txt                   # Python залежності
├── CLAUDE.md                         # Інструкції для Claude Code
├── README.md                         # Ця документація
├── Відомості про справи про банкрутство.csv  # CSV файл з даними
├── analiz_klientiv/                  # Django проєкт
│   ├── __init__.py
│   ├── settings.py                   # Налаштування Django
│   ├── urls.py                       # URL роутинг
│   ├── wsgi.py
│   └── asgi.py
└── bankruptcy/                       # Django застосунок
    ├── __init__.py
    ├── admin.py                      # Django admin налаштування
    ├── apps.py
    ├── models.py                     # Моделі даних
    ├── views.py                      # Views (контролери)
    ├── urls.py                       # URL роутинг застосунку
    └── management/
        └── commands/
            └── load_bankruptcy_data.py  # Команда завантаження даних
```

## Моделі даних

### Court (Суди)
- `name`: Назва суду

### Company (Підприємства)
- `edrpou`: Код ЄДРПОУ (унікальний)
- `name`: Назва підприємства

### BankruptcyCase (Справи про банкрутство)
- `number`: Порядковий номер (унікальний)
- `date`: Дата провадження
- `type`: Тип провадження
- `company`: Зв'язок з підприємством
- `case_number`: Номер справи
- `start_date_auc`: Дата початку аукціону (опціонально)
- `end_date_auc`: Дата закінчення аукціону (опціонально)
- `court`: Зв'язок з судом
- `end_registration_date`: Дата закінчення реєстрації (опціонально)
- `created_at`: Дата створення запису
- `updated_at`: Дата останнього оновлення

## Функціональність

### Завантаження даних
- **Повне завантаження**: Видаляє всі попередні дані та завантажує нові
- **Інкрементальне оновлення**: Додає тільки нові записи (перевіряє за номером справи)

### Веб-інтерфейс
- Головна сторінка з статистикою
- Список справ з фільтрацією та пагінацією
- Детальна інформація про справу

### Django Admin
- Управління всіма моделями через адмін панель
- Пошук та фільтрація
- Можливість редагування даних

## Команди управління

### Завантаження даних
```bash
python manage.py load_bankruptcy_data [--file FILE] [--incremental]
```

### Стандартні Django команди
```bash
python manage.py runserver          # Запуск сервера
python manage.py makemigrations     # Створення міграцій
python manage.py migrate            # Застосування міграцій
python manage.py createsuperuser    # Створення адміністратора
python manage.py collectstatic      # Збирання статичних файлів
python manage.py test               # Запуск тестів
```

## Періодичне оновлення

Для автоматичного оновлення даних можна налаштувати cron:

```bash
# Редагування crontab
crontab -e

# Додати рядок для щоденного оновлення о 01:00
0 1 * * * cd /path/to/analiz_klientiv && /path/to/venv/bin/python manage.py load_bankruptcy_data --incremental
```