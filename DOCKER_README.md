# 🐳 Docker Інструкції

Це швидкий посібник для запуску проєкту **Аналіз Клієнтів** через Docker.

Повна документація: [CLAUDE.md](./CLAUDE.md)

## 🚀 Швидкий Старт (Development)

### Перший запуск

```bash
# 1. Створити .env файл
cp .env.example .env

# 2. Запустити все автоматично (збірка + міграції + довідники)
make quickstart

# 3. Відкрити браузер
# http://localhost:8000
```

**Готово!** Проєкт запущений за 3 кроки 🎉

### Подальші запуски

```bash
make up          # Запустити
make down        # Зупинити
make logs-web    # Переглянути логи Django
```

## 📋 Основні Команди

### Управління сервісами
```bash
make help        # Список всіх команд (60+)
make status      # Статус контейнерів
make stats       # CPU/RAM статистика
make restart     # Перезапустити всі сервіси
```

### Логи
```bash
make logs        # Всі логи
make logs-web    # Django
make logs-db     # PostgreSQL
make logs-monitor # File monitor
```

### Django
```bash
make shell              # Django shell
make bash               # Bash в контейнері
make migrate            # Міграції
make createsuperuser    # Створити адміна
make collectstatic      # Статичні файли
```

### Імпорт даних
```bash
make import-bankruptcy  # Імпорт справ банкрутства
make import-reference   # Імпорт довідників
make import-court-2024  # Судові рішення 2024
```

### Backup
```bash
make backup-db              # Backup бази даних
make restore-db BACKUP=file.sql  # Відновлення
```

### Очищення
```bash
make clean-logs  # Очистити логи
make rebuild     # Повна перебудова
make prune-all   # Видалити невикористані ресурси
```

## 🏭 Production Розгортання

### 1. Налаштування

```bash
# Створити production змінні
cp .env.example .env.production

# Редагувати критичні параметри
nano .env.production
```

**Обов'язково змінити:**
- `SECRET_KEY` - випадковий рядок 64 символи
- `POSTGRES_PASSWORD` - сильний пароль
- `ALLOWED_HOSTS` - ваш домен
- `CSRF_TRUSTED_ORIGINS` - https://ваш-домен.com

**Згенерувати SECRET_KEY:**
```bash
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```

### 2. SSL Сертифікати (HTTPS)

**Let's Encrypt (рекомендовано):**
```bash
sudo apt-get install certbot
sudo certbot certonly --standalone -d your-domain.com
sudo cp /etc/letsencrypt/live/your-domain.com/*.pem docker/nginx/ssl/
```

**Self-signed (тестування):**
```bash
make ssl-generate-self-signed
```

Розкоментувати HTTPS секцію в `docker/nginx/conf.d/analiz.conf`

### 3. Розгортання

```bash
# Повне автоматичне розгортання
make deploy-prod

# Створити адміна
docker compose -f docker-compose.prod.yml exec web python manage.py createsuperuser

# Перевірка
make health-check
```

### 4. Firewall

```bash
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable
```

### 5. Автоматичні Backup (crontab)

```bash
crontab -e

# Додати рядок (щоденний backup о 3:00)
0 3 * * * cd /home/ruslan/PYTHON/analiz_klientiv && make backup-db-prod

# Оновлення SSL щомісяця
0 0 1 * * certbot renew --quiet && cd /home/ruslan/PYTHON/analiz_klientiv && make restart-nginx
```

## 📊 Архітектура

### Development (7 контейнерів)
1. **db** - PostgreSQL 15
2. **redis** - Redis 7
3. **web** - Django (runserver)
4. **file_monitor** - Моніторинг CSV файлів
5. **stats_monitor** - Оновлення статистики
6. **mistral_processor** - Витягування кредиторів
7. **dedup_processor** - Дедуплікація

### Production (8 контейнерів)
- Всі вищезазначені + **nginx** (SSL, статичні файли)
- **web** використовує Gunicorn (4 workers, 2 threads)

## 🔧 Troubleshooting

### Контейнери не запускаються
```bash
make logs
make health-check
docker ps -a
```

### PostgreSQL не готовий
```bash
make logs-db
docker compose exec db pg_isready -U analiz_user
```

### Порти зайняті
```bash
sudo lsof -i :8000  # Django
sudo lsof -i :5432  # PostgreSQL
```

### Недостатньо місця
```bash
make disk-usage
make prune-all
```

### Nginx 502 Bad Gateway
```bash
make logs-nginx
make logs-web
make restart-prod
```

## 📁 Структура Файлів

```
/home/ruslan/PYTHON/analiz_klientiv/
├── docker-compose.yml          # Development
├── docker-compose.prod.yml     # Production
├── .env                        # Dev змінні
├── .env.production            # Prod змінні
├── Makefile                   # 60+ команд
├── docker/
│   ├── django/
│   │   ├── Dockerfile         # Multi-stage build
│   │   ├── entrypoint.sh      # Ініціалізація
│   │   └── wait-for-it.sh     # Очікування PostgreSQL
│   └── nginx/
│       ├── nginx.conf         # Nginx конфігурація
│       ├── conf.d/
│       │   └── analiz.conf    # Додаток конфігурація
│       └── ssl/               # SSL сертифікати
└── backups/                   # Backup файли
```

## 🎯 Volumes

**Development:**
- `postgres_data` - База даних
- `redis_data` - Redis
- `static_volume` - Статичні файли
- `logs_volume` - Логи

**Production:**
- `postgres_data_prod`
- `redis_data_prod`
- `static_volume_prod`
- `media_volume_prod`
- `logs_volume_prod`

**Важливо:** Volumes зберігаються після `docker compose down`

## 📚 Додаткові Ресурси

- **Повна документація**: [CLAUDE.md](./CLAUDE.md)
- **Docker Compose**: https://docs.docker.com/compose/
- **Django**: https://docs.djangoproject.com/
- **PostgreSQL**: https://www.postgresql.org/docs/
- **Nginx**: https://nginx.org/ru/docs/

## 🔐 Безпека (Production)

**Чеклист:**
- ✅ Змінити SECRET_KEY
- ✅ Сильні паролі для PostgreSQL
- ✅ Налаштувати SSL/TLS
- ✅ Обмежити ALLOWED_HOSTS
- ✅ Firewall (ufw)
- ✅ Регулярні backup
- ✅ Оновлювати залежності
- ⚠️ Не комітити .env в Git
- ⚠️ Обмежити SSH доступ
- ⚠️ Налаштувати fail2ban

## 🆘 Підтримка

**Логи:**
```bash
make logs        # Всі логи
make logs-web    # Django
make logs-db     # PostgreSQL
make logs-nginx  # Nginx (production)
```

**Здоров'я сервісів:**
```bash
make health-check
make status
make stats
```

**Backup:**
```bash
make backup-db
make db-backup-all  # Повний backup
```

---

**Автор:** Analiz Klientiv Team
**Версія Docker:** 29.1.3
**Версія Docker Compose:** 5.0.0
**Останнє оновлення:** 2025-12-23
