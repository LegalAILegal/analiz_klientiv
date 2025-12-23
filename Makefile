.PHONY: help build up down logs shell migrate quickstart

# Docker Compose команда (нова версія використовує 'docker compose')
DOCKER_COMPOSE = docker compose
DOCKER_COMPOSE_PROD = docker compose -f docker-compose.prod.yml

# Кольори для виводу
BLUE=\033[0;34m
GREEN=\033[0;32m
RED=\033[0;31m
YELLOW=\033[0;33m
NC=\033[0m

help: ## Показати це повідомлення
	@echo "$(BLUE)Доступні команди Docker:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# ========== Development Commands ==========

build: ## Збудувати Docker образи (dev)
	@echo "$(BLUE)Збірка Docker образів...$(NC)"
	${DOCKER_COMPOSE} build

up: ## Запустити всі сервіси (dev)
	@echo "$(GREEN)Запуск development середовища...$(NC)"
	${DOCKER_COMPOSE} up -d
	@echo "$(GREEN)✓ Сервіси запущено! Web: http://localhost:8000$(NC)"

up-logs: ## Запустити сервіси з логами
	${DOCKER_COMPOSE} up

down: ## Зупинити всі сервіси
	@echo "$(RED)Зупинка контейнерів...$(NC)"
	${DOCKER_COMPOSE} down

restart: ## Перезапустити сервіси
	${DOCKER_COMPOSE} restart

status: ## Показати статус контейнерів
	${DOCKER_COMPOSE} ps

stats: ## Показати статистику ресурсів
	docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"

# ========== Production Commands ==========

build-prod: ## Збудувати образи (production)
	${DOCKER_COMPOSE_PROD} build

up-prod: ## Запустити production
	${DOCKER_COMPOSE_PROD} up -d

down-prod: ## Зупинити production
	${DOCKER_COMPOSE_PROD} down

# ========== Logs Commands ==========

logs: ## Всі логи
	${DOCKER_COMPOSE} logs -f

logs-web: ## Логи Django web
	${DOCKER_COMPOSE} logs -f web

logs-monitor: ## Логи file_monitor
	${DOCKER_COMPOSE} logs -f file_monitor

logs-stats: ## Логи stats_monitor
	${DOCKER_COMPOSE} logs -f stats_monitor

logs-mistral: ## Логи mistral_processor
	${DOCKER_COMPOSE} logs -f mistral_processor

logs-dedup: ## Логи dedup_processor
	${DOCKER_COMPOSE} logs -f dedup_processor

logs-db: ## Логи PostgreSQL
	${DOCKER_COMPOSE} logs -f db

# ========== Shell Commands ==========

shell: ## Django shell
	${DOCKER_COMPOSE} exec web python manage.py shell

bash: ## Bash в web контейнері
	${DOCKER_COMPOSE} exec web bash

db-shell: ## PostgreSQL shell
	${DOCKER_COMPOSE} exec db psql -U analiz_user -d analiz_klientiv

redis-cli: ## Redis CLI
	${DOCKER_COMPOSE} exec redis redis-cli

# ========== Django Commands ==========

migrate: ## Виконати міграції
	${DOCKER_COMPOSE} exec web python manage.py migrate

makemigrations: ## Створити міграції
	${DOCKER_COMPOSE} exec web python manage.py makemigrations

collectstatic: ## Зібрати статичні файли
	${DOCKER_COMPOSE} exec web python manage.py collectstatic --noinput

createsuperuser: ## Створити суперкористувача
	${DOCKER_COMPOSE} exec web python manage.py createsuperuser

# ========== Import Commands ==========

import-bankruptcy: ## Імпорт даних банкрутства
	${DOCKER_COMPOSE} exec web python manage.py load_bankruptcy_data

import-reference: ## Імпорт довідкових даних
	${DOCKER_COMPOSE} exec web python manage.py import_reference_data --force

import-court-2024: ## Імпорт судових рішень 2024
	${DOCKER_COMPOSE} exec web python manage.py import_court_decisions --year 2024 --batch-size 5000

# ========== Backup Commands ==========

backup-db: ## Резервна копія PostgreSQL
	@echo "$(BLUE)Створення backup...$(NC)"
	${DOCKER_COMPOSE} exec -T db pg_dump -U analiz_user analiz_klientiv > backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup створено!$(NC)"

restore-db: ## Відновлення з backup (BACKUP=файл.sql)
	@echo "$(RED)Відновлення з $(BACKUP)...$(NC)"
	${DOCKER_COMPOSE} exec -T db psql -U analiz_user analiz_klientiv < $(BACKUP)

# ========== Cleanup Commands ==========

clean: ## Очистити volumes (НЕБЕЗПЕЧНО!)
	@echo "$(RED)УВАГА: Це видалить всі дані! Продовжити? [y/N]$(NC)" && read ans && [ $${ans:-N} = y ]
	${DOCKER_COMPOSE} down -v
	docker system prune -f

clean-logs: ## Очистити логи
	${DOCKER_COMPOSE} exec web rm -rf /app/logs/*.log

rebuild: ## Повна перебудова
	${DOCKER_COMPOSE} down
	${DOCKER_COMPOSE} build --no-cache
	${DOCKER_COMPOSE} up -d

# ========== Quick Start ==========

quickstart: ## Швидкий старт (build + up + migrate)
	@echo "$(GREEN)Швидкий старт проєкту...$(NC)"
	make build
	make up
	@echo "$(BLUE)Очікування готовності БД...$(NC)"
	sleep 15
	make migrate
	make import-reference
	@echo "$(GREEN)✓ Готово! http://localhost:8000$(NC)"

# ========== Monitoring ==========

monitor-start: ## Запустити file_monitor
	${DOCKER_COMPOSE} start file_monitor

monitor-stop: ## Зупинити file_monitor
	${DOCKER_COMPOSE} stop file_monitor

stats-start: ## Запустити stats_monitor
	${DOCKER_COMPOSE} start stats_monitor

stats-stop: ## Зупинити stats_monitor
	${DOCKER_COMPOSE} stop stats_monitor

mistral-start: ## Запустити mistral_processor
	${DOCKER_COMPOSE} start mistral_processor

mistral-stop: ## Зупинити mistral_processor
	${DOCKER_COMPOSE} stop mistral_processor

dedup-start: ## Запустити dedup_processor
	${DOCKER_COMPOSE} start dedup_processor

dedup-stop: ## Зупинити dedup_processor
	${DOCKER_COMPOSE} stop dedup_processor

# ========== Production Specific ==========

logs-prod: ## Всі логи (production)
	${DOCKER_COMPOSE_PROD} logs -f

logs-nginx: ## Логи Nginx (production)
	${DOCKER_COMPOSE_PROD} logs -f nginx

restart-nginx: ## Перезапустити Nginx (production)
	${DOCKER_COMPOSE_PROD} restart nginx

restart-prod: ## Перезапустити всі сервіси (production)
	${DOCKER_COMPOSE_PROD} restart

status-prod: ## Статус контейнерів (production)
	${DOCKER_COMPOSE_PROD} ps

ssl-check: ## Перевірити SSL сертифікати
	@echo "$(BLUE)Перевірка SSL сертифікатів...$(NC)"
	@if [ -f docker/nginx/ssl/fullchain.pem ]; then \
		openssl x509 -in docker/nginx/ssl/fullchain.pem -noout -dates; \
	else \
		echo "$(RED)SSL сертифікати не знайдено!$(NC)"; \
	fi

ssl-generate-self-signed: ## Створити self-signed SSL сертифікат (тільки для тестування)
	@echo "$(BLUE)Створення self-signed SSL сертифіката...$(NC)"
	openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
		-keyout docker/nginx/ssl/privkey.pem \
		-out docker/nginx/ssl/fullchain.pem \
		-subj "/C=UA/ST=Kyiv/L=Kyiv/O=AnalizKlientiv/CN=localhost"
	chmod 644 docker/nginx/ssl/*.pem
	@echo "$(GREEN)✓ Self-signed сертифікат створено!$(NC)"

# ========== Deployment Commands ==========

deploy-prod: ## Повне розгортання production (build + migrate + up)
	@echo "$(GREEN)Розгортання production середовища...$(NC)"
	make build-prod
	${DOCKER_COMPOSE_PROD} up -d db redis
	@echo "$(BLUE)Очікування готовності БД...$(NC)"
	sleep 20
	make migrate-prod
	make collectstatic-prod
	${DOCKER_COMPOSE_PROD} up -d
	@echo "$(GREEN)✓ Production розгорнуто!$(NC)"

migrate-prod: ## Виконати міграції (production)
	${DOCKER_COMPOSE_PROD} exec web python manage.py migrate

collectstatic-prod: ## Зібрати статичні файли (production)
	${DOCKER_COMPOSE_PROD} exec web python manage.py collectstatic --noinput

backup-db-prod: ## Резервна копія PostgreSQL (production)
	@echo "$(BLUE)Створення production backup...$(NC)"
	${DOCKER_COMPOSE_PROD} exec -T db pg_dump -U analiz_user analiz_klientiv > backups/backup_prod_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Production backup створено в backups/$(NC)"

restore-db-prod: ## Відновлення з backup (production) (BACKUP=файл.sql)
	@echo "$(RED)Відновлення production БД з $(BACKUP)...$(NC)"
	${DOCKER_COMPOSE_PROD} exec -T db psql -U analiz_user analiz_klientiv < $(BACKUP)

# ========== Utility Commands ==========

ps-all: ## Показати всі Docker контейнери
	docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

images: ## Показати Docker образи
	docker images | grep analiz

volumes: ## Показати Docker volumes
	docker volume ls | grep analiz

networks: ## Показати Docker мережі
	docker network ls | grep analiz

disk-usage: ## Показати використання диску Docker
	docker system df -v

prune-all: ## Очистити невикористані Docker ресурси
	@echo "$(YELLOW)Очищення невикористаних Docker ресурсів...$(NC)"
	docker system prune -a --volumes -f
	@echo "$(GREEN)✓ Очищення завершено!$(NC)"

# ========== Testing Commands ==========

test: ## Запустити тести
	${DOCKER_COMPOSE} exec web python manage.py test

test-coverage: ## Запустити тести з coverage
	${DOCKER_COMPOSE} exec web coverage run --source='.' manage.py test
	${DOCKER_COMPOSE} exec web coverage report
	${DOCKER_COMPOSE} exec web coverage html

# ========== Database Commands ==========

db-backup-all: ## Backup всіх даних (БД + volumes)
	@echo "$(BLUE)Створення повного backup...$(NC)"
	mkdir -p backups/full_backup_$$(date +%Y%m%d_%H%M%S)
	make backup-db
	docker run --rm -v analiz_klientiv_postgres_data:/data -v $$(pwd)/backups:/backup alpine tar czf /backup/full_backup_$$(date +%Y%m%d_%H%M%S)/postgres_data.tar.gz -C /data .
	@echo "$(GREEN)✓ Повний backup створено!$(NC)"

db-reset: ## Скинути базу даних (НЕБЕЗПЕЧНО!)
	@echo "$(RED)УВАГА: Це видалить всі дані з БД! Продовжити? [y/N]$(NC)" && read ans && [ $${ans:-N} = y ]
	${DOCKER_COMPOSE} exec db psql -U analiz_user -d postgres -c "DROP DATABASE IF EXISTS analiz_klientiv;"
	${DOCKER_COMPOSE} exec db psql -U analiz_user -d postgres -c "CREATE DATABASE analiz_klientiv;"
	make migrate

# ========== Maintenance ==========

update-deps: ## Оновити залежності Python
	${DOCKER_COMPOSE} exec web pip install --upgrade pip
	${DOCKER_COMPOSE} exec web pip install -r requirements.txt --upgrade

freeze-deps: ## Заморозити поточні версії залежностей
	${DOCKER_COMPOSE} exec web pip freeze > requirements.txt

health-check: ## Перевірка здоров'я всіх сервісів
	@echo "$(BLUE)Перевірка здоров'я сервісів...$(NC)"
	@docker ps --filter "name=analiz" --format "table {{.Names}}\t{{.Status}}"
	@echo ""
	@${DOCKER_COMPOSE} exec -T db pg_isready -U analiz_user && echo "$(GREEN)✓ PostgreSQL: OK$(NC)" || echo "$(RED)✗ PostgreSQL: FAIL$(NC)"
	@${DOCKER_COMPOSE} exec -T redis redis-cli ping > /dev/null && echo "$(GREEN)✓ Redis: OK$(NC)" || echo "$(RED)✗ Redis: FAIL$(NC)"
	@curl -sf http://localhost:8000/ > /dev/null && echo "$(GREEN)✓ Django Web: OK$(NC)" || echo "$(RED)✗ Django Web: FAIL$(NC)"

# ========== Development Helpers ==========

watch-logs-web: ## Live логи Django (з кольорами)
	${DOCKER_COMPOSE} logs -f --tail=100 web

watch-logs-all: ## Live логи всіх сервісів
	${DOCKER_COMPOSE} logs -f --tail=50

restart-web: ## Швидкий перезапуск web
	${DOCKER_COMPOSE} restart web

enter-web: ## Увійти в web контейнер
	${DOCKER_COMPOSE} exec web /bin/bash

enter-db: ## Увійти в db контейнер
	${DOCKER_COMPOSE} exec db /bin/bash
