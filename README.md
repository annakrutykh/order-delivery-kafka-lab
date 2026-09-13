# order-delivery-kafka-lab

Учебный мини-стенд для одного занятия по Kafka + CI/CD для ручных
QA-тестировщиков. Полностью самостоятельный репозиторий — не связан
с продуктовым TaskFlow-курсом ни кодом, ни историей.

Документация:

- [`docs/DESIGN.md`](docs/DESIGN.md) — техническая спека: сервисы,
  инфраструктура, багы, git/CI-структура. Основа для реализации.
- [`docs/LESSON_PLAN.md`](docs/LESSON_PLAN.md) — план занятия для
  ведущего: теория, чек-листы, команды, сценарий фикс→ретест, тайминг.

Статус: реализация завершена и проверена — оба сервиса (`order-api`,
`delivery-worker`), docker-compose-стенд, GitHub Actions CI и обе
фиче-ветки с багами (`feature/producer-bugs`, `feature/consumer-bug`)
и их fix-коммитами на месте.

## Как поднять стенд

```bash
docker compose up --build -d
```

Точки входа после старта:

- **order-api** — Swagger UI: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8081
- **Kibana**: http://localhost:5601
- **Postgres** (для DBeaver и т.п.): `localhost:5432`, база `orders_db`,
  пользователь `orders_user`, пароль `orders_password`

`POST /admin/reset` пересеивает Postgres тем же сидом (для прогонов/
репетиций ведущего, не для студентов). Он **не** трогает Kafka —
подробности и ограничения см. в `docs/LESSON_PLAN.md`.
