#!/bin/bash
set -e

echo "=== Django Entrypoint ==="

# Очікування готовності PostgreSQL
echo "Очікування PostgreSQL на $POSTGRES_HOST:${POSTGRES_PORT:-5432}..."
/app/docker/django/wait-for-it.sh $POSTGRES_HOST:${POSTGRES_PORT:-5432} --timeout=60 --strict

echo "✓ PostgreSQL готовий!"

# Виконуємо міграції ТІЛЬКИ для web контейнера
# Для моніторингу та процесорів НЕ виконуємо міграції (уникаємо конфліктів)
if [[ "$@" != *"start_file_monitor"* ]] && \
   [[ "$@" != *"auto_update_statistics"* ]] && \
   [[ "$@" != *"analyze_resolutions"* ]]; then

    echo "Виконання міграцій бази даних..."
    python manage.py migrate --noinput || {
        echo "⚠️  Помилка міграцій (можливо вже виконано іншим контейнером)"
    }

    # Збірка статичних файлів для production
    if [ "$DEBUG" = "False" ] || [ "$DEBUG" = "false" ]; then
        echo "Збір статичних файлів..."
        python manage.py collectstatic --noinput --clear || {
            echo "⚠️  Помилка collectstatic"
        }
    fi
fi

# Запуск переданої команди
echo "Запуск команди: $@"
exec "$@"
