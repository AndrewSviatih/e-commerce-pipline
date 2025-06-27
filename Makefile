.PHONY: setup-local generate-data build up down logs init-airflow clean

# === Локальная настройка ===
setup-local:
	@echo "--- Создание локального виртуального окружения... ---"
	python3 -m venv .venv
	@echo "--- Установка локальных зависимостей... ---"
	./.venv/bin/pip install -r requirements-local.txt

generate-data:
	@echo "--- Запуск скрипта генерации данных... ---"
	./.venv/bin/python generate_data.py

# === Управление Docker Compose ===
build:
	@echo "--- Сборка Docker-образов... ---"
	docker-compose build

up:
	@echo "--- Запуск всех сервисов... ---"
	AIRFLOW_UID=$(shell id -u) docker-compose up -d

down:
	@echo "--- Остановка всех сервисов... ---"
	docker-compose down

logs:
	@echo "--- Просмотр логов для сервиса: $(service) ---"
	docker-compose logs -f $(service)

# === Инициализация ===
init-airflow:
	@echo "--- Инициализация Airflow... ---"
	docker-compose exec postgres psql -U user -d ecom_db -c "CREATE DATABASE airflow_db;"
	AIRFLOW_UID=$(shell id -u) docker-compose up airflow-init
	@echo "--- Инициализация Airflow завершена. ---"

# Полная очистка
clean:
	@echo "--- Полная очистка проекта... ---"
	docker-compose down -v --remove-orphans
	rm -rf .venv