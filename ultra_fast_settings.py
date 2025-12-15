# 🚀 УЛЬТРА-ШВИДКІ НАЛАШТУВАННЯ ДЛЯ ВИТЯГУВАННЯ РЕЗОЛЮТИВНИХ ЧАСТИН
# Використовувати: python manage.py extract_resolution_texts_ultra_fast --settings=ultra_fast_settings

from .settings import *

# 🔥 МАКСИМАЛЬНІ ПАРАМЕТРИ ПРОДУКТИВНОСТІ ДЛЯ FASTRESOLUTIONEXTRACTOR

# Багатопоточність - КРИТИЧНО ВАЖЛИВО для швидкодії
RESOLUTION_MAX_WORKERS = 200        # МАКСИМУМ потоків (було 20)
RESOLUTION_BATCH_SIZE = 2000        # МАКСИМУМ за один запуск (було 200) 
RESOLUTION_SUB_BATCH_SIZE = 300     # МАКСИМУМ під-батчі (було 50)
RESOLUTION_MINI_BATCH_SIZE = 100    # МАКСИМУМ міні-батчі (було 20)

# Мережеві оптимізації - КРИТИЧНО для зменшення затримок
RESOLUTION_DOWNLOAD_TIMEOUT = 8     # Швидкі таймаути (було 30)
REQUEST_DELAY = 0.005               # Мінімальні затримки (було 0.05)

# Тимчасові файли - оптимізовано для швидкості
TEMP_DIR = "/tmp/ultra_fast_resolution"

# Налаштування бази даних для максимальної швидкості
DATABASES["default"].update({
    "CONN_MAX_AGE": 600,            # Тривалі з"єднання
    "OPTIONS": {
        "MAX_CONNS": 50,            # Більше з"єднань
        "connect_timeout": 5,       # Швидкі підключення
    }
})

# Логування - мінімальне для швидкості в продакшні
if not DEBUG:
    LOGGING["loggers"]["fast_resolution_extractor"] = {
        "handlers": ["file"],
        "level": "WARNING",  # Тільки попередження та помилки
        "propagate": False,
    }

# Кешування - агресивне для повторних запитів
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "ultra-fast-cache",
        "TIMEOUT": 3600,  # 1 година кеш для статистик
        "OPTIONS": {
            "MAX_ENTRIES": 10000,
        }
    }
}

# Django налаштування для продуктивності
DEBUG = False
ALLOWED_HOSTS = ["*"]

# Відключаємо middleware що не потрібні для команд
if "runserver" not in sys.argv:
    MIDDLEWARE = [
        "django.middleware.security.SecurityMiddleware",
        "django.middleware.common.CommonMiddleware",
    ]

# Статичні файли - оптимізовано
STATICFILES_STORAGE = "django.contrib.staticfiles.storage.StaticFilesStorage"

print("""
🚀 УЛЬТРА-ШВИДКІ НАЛАШТУВАННЯ АКТИВОВАНО:
   - Потоків: {workers}
   - Батч: {batch} 
   - Під-батч: {sub_batch}
   - Міні-батч: {mini_batch}
   - Таймаут: {timeout}с
   - Затримка: {delay}с
   
💡 РЕКОМЕНДАЦІЇ:
   - Використовуй --ultra-mode для максимуму
   - Моніторь системні ресурси
   - При помилках зменш max_workers
""".format(
    workers=RESOLUTION_MAX_WORKERS,
    batch=RESOLUTION_BATCH_SIZE, 
    sub_batch=RESOLUTION_SUB_BATCH_SIZE,
    mini_batch=RESOLUTION_MINI_BATCH_SIZE,
    timeout=RESOLUTION_DOWNLOAD_TIMEOUT,
    delay=REQUEST_DELAY
))