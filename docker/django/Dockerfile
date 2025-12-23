# ========================================
# Stage 1: Builder
# ========================================
FROM python:3.12-slim AS builder

# Встановлення системних залежностей для компіляції
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Створення робочої директорії
WORKDIR /app

# Копіювання requirements.txt
COPY requirements.txt .

# Компіляція Python wheels (для швидшої встановки)
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# ========================================
# Stage 2: Runtime
# ========================================
FROM python:3.12-slim

# Встановлення тільки runtime залежностей
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Створення робочої директорії
WORKDIR /app

# Копіювання Python wheels з builder stage
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Встановлення Python залежностей з wheels
RUN pip install --no-cache /wheels/* && rm -rf /wheels

# Створення структури директорій
RUN mkdir -p /app/logs \
    /app/staticfiles \
    /app/data \
    /tmp/resolution_temp

# Копіювання коду проєкту
COPY . /app/

# ВАЖЛИВО: Копіювання 26 GB CSV даних в образ
# (згідно з вимогою користувача для автономності)
COPY data/*.csv /app/data/

# Налаштування прав доступу на entrypoint
RUN chmod +x /app/docker/django/entrypoint.sh \
    && chmod +x /app/docker/django/wait-for-it.sh

# Налаштування змінних середовища
ENV DJANGO_SETTINGS_MODULE=analiz_klientiv.settings
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Порт Django
EXPOSE 8000

# Entrypoint для ініціалізації
ENTRYPOINT ["/app/docker/django/entrypoint.sh"]

# За замовчуванням запускається Gunicorn (для production)
# Для dev буде перевизначено в docker-compose.yml
CMD ["gunicorn", "analiz_klientiv.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4", "--threads", "2", "--timeout", "120"]
