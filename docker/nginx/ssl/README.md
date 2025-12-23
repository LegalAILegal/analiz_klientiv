# SSL Сертифікати

Ця директорія призначена для зберігання SSL сертифікатів для HTTPS.

## Налаштування SSL для production

### Варіант 1: Let's Encrypt (Рекомендовано)

```bash
# Встановіть certbot
sudo apt-get install certbot

# Отримайте сертифікат
sudo certbot certonly --standalone -d your-domain.com -d www.your-domain.com

# Скопіюйте сертифікати
sudo cp /etc/letsencrypt/live/your-domain.com/fullchain.pem ./fullchain.pem
sudo cp /etc/letsencrypt/live/your-domain.com/privkey.pem ./privkey.pem
sudo chmod 644 fullchain.pem
sudo chmod 644 privkey.pem
```

### Варіант 2: Self-signed сертифікат (Тільки для тестування!)

```bash
# Створення self-signed сертифіката
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout ./privkey.pem \
  -out ./fullchain.pem \
  -subj "/C=UA/ST=Kyiv/L=Kyiv/O=YourCompany/CN=localhost"

chmod 644 fullchain.pem privkey.pem
```

### Структура файлів

Після налаштування SSL, директорія повинна містити:

```
ssl/
├── fullchain.pem   # Повний ланцюг сертифікатів
├── privkey.pem     # Приватний ключ
└── README.md       # Цей файл
```

### Активація HTTPS

1. Розмістіть сертифікати в цій директорії
2. Розкоментуйте HTTPS секцію в `docker/nginx/conf.d/analiz.conf`
3. Оновіть `ALLOWED_HOSTS` та `CSRF_TRUSTED_ORIGINS` в `.env.production`
4. Перезапустіть nginx:

```bash
make restart-nginx  # або docker-compose -f docker-compose.prod.yml restart nginx
```

## Автоматичне оновлення Let's Encrypt

Додайте cron job для автоматичного оновлення:

```bash
# Відкрийте crontab
crontab -e

# Додайте рядок (оновлення щомісяця)
0 0 1 * * certbot renew --quiet && docker-compose -f /path/to/docker-compose.prod.yml restart nginx
```

## Безпека

⚠️ **ВАЖЛИВО:**
- Ніколи не комітьте приватні ключі в Git
- Обмежте права доступу: `chmod 600 privkey.pem`
- Регулярно оновлюйте сертифікати
- Використовуйте тільки сертифікати від довірених центрів для production