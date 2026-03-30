SHELL := /bin/bash

.PHONY: airflow-init airflow-up airflow-down dashboard dashboard-down test

airflow-init:
	docker compose up airflow-init

airflow-up:
	docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer

airflow-down:
	docker compose down

dashboard:
	docker compose up -d dashboard

dashboard-down:
	docker compose stop dashboard

test:
	pytest
