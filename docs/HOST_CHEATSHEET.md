# Шпаргалка ведущего — команды и доступы

Короткая памятка для занятия. Полные версии — `docs/LESSON_PLAN.md` (план
занятия), `docs/TESTER_TASKS.md` (задачи для тестировщика),
`docs/KAFKA_TESTING_THEORY.md` (теория).

Запросы к `order-api` — через **Swagger** (`http://localhost:8000/docs`,
кнопка **Try it out** → вставить тело → **Execute**). Проверки — через
терминал (Kafka, БД) и Kibana (браузер).

## Доступы

На репетиции (у себя на машине) — все адреса `localhost`. На занятии
(доступ у студентов через ngrok/cloudflared) — вместо `localhost`
подставляется адрес туннеля, который даёшь студентам в чат.

| Сервис | Адрес | Логин / пароль |
|---|---|---|
| Swagger `order-api` | `http://localhost:8000/docs` | — |
| Kafka UI | `http://localhost:8081` | — |
| Kibana | `http://localhost:5601` | — |
| Postgres (DBeaver) | `localhost:5432` | база `orders_db`, пользователь `orders_user`, пароль `orders_password` |

**Kibana data view:** index pattern `app-logs-*`, time field `@timestamp`.

## Перед занятием

```bash
docker compose ps
```
Все сервисы — `Up` (kafka и postgres — ещё и `healthy`).

## Если что-то пошло не так

```bash
docker compose down -v && docker compose up --build -d
```

---

# ВЕТКА 1: feature/producer-bugs (сервис `order-api`)

PR: **https://github.com/annakrutykh/order-delivery-kafka-lab/pull/1**

## 1. Подтянуть ветку с багом и пересобрать `order-api`

```bash
git fetch origin
git checkout feature/producer-bugs
git pull
git log --oneline -5
docker compose up --build -d order-api
```

## 2. Показать PR студентам

Открыть https://github.com/annakrutykh/order-delivery-kafka-lab/pull/1

- **Conversation** — внизу блок чеков, **все зелёные**.
- **Checks** — клик на джоб `order-api`, лог линта и тестов — все шаги зелёные.
- **Files changed** — диф в `order-api/app/main.py`. Здесь физически лежат
  все 3 бага этой ветки.

Вывод для студентов: пайплайн зелёный, код смёржился бы без вопросов —
дальше проверяем руками.

## 3. Баг 1 — дубль сообщения (обычный переход статуса)

**В Swagger:**

1. `POST /orders` — тело:
   ```json
   {"customer_name": "Test", "customer_address": "Test"}
   ```
   Execute → взять `id` из ответа.

2. `PATCH /orders/{order_id}/status` — `order_id` = тот id, тело:
   ```json
   {"status": "IN_PROGRESS"}
   ```

3. Тот же `PATCH /orders/{order_id}/status` — тот же `order_id`, тело:
   ```json
   {"status": "READY_FOR_DELIVERY"}
   ```

**В терминале** (подставить свой id вместо `<ID>`):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep '"correlationId": "<ID>"'
```
**Ожидание:** 1 сообщение. **Баг:** 2 одинаковых сообщения.

## 4. Баг 2 — битое сообщение (переход напрямую, минуя `IN_PROGRESS`)

**В Swagger:**

1. `POST /orders` — тело:
   ```json
   {"customer_name": "Test", "customer_address": "Test"}
   ```
   Execute → взять `id`.

2. `PATCH /orders/{order_id}/status` — тот `order_id`, тело сразу:
   ```json
   {"status": "READY_FOR_DELIVERY"}
   ```
   (без промежуточного `IN_PROGRESS`)

3. `GET /orders/{order_id}/delivery` — тот же `order_id`. **Ожидание:** `200` с данными. **Баг:** `404`.

**В терминале:**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep '"correlationId": "<ID>"'
```
**Ожидание:** сообщение с полями `orderId`, `occurredAt`. **Баг:** этих полей нет.

```bash
docker compose logs delivery-worker --since 2m 2>&1 | grep '<ID>'
```
**Ожидание:** ничего. **Баг:** строка `ERROR ... failed to process message`.

## 5. Баг 3 — неполный батч (`dispatch-batch`)

**В терминале, до вызова ручки:**
```bash
docker compose exec postgres psql -U orders_user -d orders_db \
  -c "SELECT count(*) FROM orders WHERE status = 'READY_FOR_DELIVERY';"
```
Запомнить число — это **N**.

**В Swagger:** `POST /orders/dispatch-batch` (тело не нужно) → Execute → посмотреть `dispatchedCount` в ответе.

**В терминале:**
```bash
docker compose logs order-api --since 1m 2>&1 | grep -c "dispatching order to"
```
**Ожидание:** `dispatchedCount` = N = число строк здесь.
**Баг:** `dispatchedCount` = N (совпадает с БД), но строк здесь — N−1.

## 6. Запушить fix и подтянуть

```bash
git push origin feature/producer-bugs
git pull
docker compose up --build -d order-api
```

## 7. Показать PR снова

Обновить страницу https://github.com/annakrutykh/order-delivery-kafka-lab/pull/1

- **Conversation** — новый коммит в списке, чеки **снова зелёные** (были
  зелёными и на баге, и на фиксе — тесты не видят эту область вообще).
- **Files changed** — клик на новый коммит отдельно, увидеть fix-diff.

## 8. Ретест — баги 1-3 на новых заказах

Повторить шаги 3, 4, 5 целиком на новых заказах (новые вызовы `POST /orders`
в Swagger, новые id). **Ожидание теперь везде без бага:** 1 сообщение на
переход, доставка создаётся при skip-переходе, `dispatchedCount` совпадает
с реальным числом сообщений.

---

# ВЕТКА 2: feature/consumer-bug (сервис `delivery-worker`)

PR: **https://github.com/annakrutykh/order-delivery-kafka-lab/pull/2**

## 1. Подтянуть ветку с багом и пересобрать `delivery-worker`

```bash
git fetch origin
git checkout feature/consumer-bug
git pull
git log --oneline -5
docker compose up --build -d delivery-worker
```

## 2. Показать PR студентам

Открыть https://github.com/annakrutykh/order-delivery-kafka-lab/pull/2

- **Conversation** — чеки внизу зелёные.
- **Checks** — оба джоба (`order-api`, `delivery-worker`) прошли.
- **Files changed** — диф в `delivery-worker/worker/main.py` и `worker/db.py`.
  Заголовок коммита звучит как безобидная оптимизация («reduce commit
  overhead») — не как баг. Стоит это проговорить студентам: заголовок
  коммита не гарантирует, что внутри нет проблемы.

## 3. Баг 4 — дубль доставки после рестарта воркера

**В терминале — рестарт сразу перед началом** (сбрасывает таймер автокоммита, сейчас 2 минуты):
```bash
docker compose restart delivery-worker
```

**Сразу после (в течение 15-20 секунд), в Swagger:**

1. `POST /orders` — тело:
   ```json
   {"customer_name": "Test", "customer_address": "Test"}
   ```
   Execute → взять `id`.

2. `PATCH /orders/{order_id}/status` — тот `order_id`, тело:
   ```json
   {"status": "IN_PROGRESS"}
   ```

3. Тот же `PATCH`, тот же `order_id`, тело:
   ```json
   {"status": "READY_FOR_DELIVERY"}
   ```

**В терминале, проверка «до»:**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep -c '"correlationId": "<ID>"'

docker compose exec postgres psql -U orders_user -d orders_db \
  -c "SELECT * FROM deliveries WHERE order_id = <ID>;"
```
**Ожидание:** 1 сообщение, 1 запись.

**В терминале — рестарт «передеплоя»** (не позже ~1.5 минут после обработки):
```bash
docker compose restart delivery-worker
```

Подождать 30-60 секунд, затем повторить те же две команды проверки.
**Ожидание:** сообщений по-прежнему 1. **Баг:** записей в `deliveries` стало 2.

## 4. Запушить fix и подтянуть

```bash
git push origin feature/consumer-bug
git pull
docker compose up --build -d delivery-worker
```

## 5. Показать PR снова

Обновить страницу https://github.com/annakrutykh/order-delivery-kafka-lab/pull/2

- **Conversation** — новый коммит, чеки снова зелёные.
- **Files changed** — клик на новый коммит, увидеть fix-diff.

## 6. Ретест — баг 4 на новом заказе

В Swagger создать новый заказ и провести до `READY_FOR_DELIVERY` (шаги как
в п.3). Проверить в терминале (1 сообщение, 1 запись). Сделать
`docker compose restart delivery-worker`, подождать 20-30 секунд, проверить
`deliveries` для этого заказа снова — **ожидание теперь: по-прежнему 1**
запись (спешить с таймингом больше не нужно — фикс не зависит от интервала
автокоммита).

---

# Справочно: где смотреть результат вручную (для студентов)

**Kafka UI** (`localhost:8081`) → топик → Messages → поле Search:
```
correlationId": "<id>"
```

**Kibana** (`localhost:5601`) → Discover → дата-вью → KQL-фильтр:
```
correlationId: "<id>"
```
Добавить колонку `name` (различает `order-api` / `delivery-worker`). Для
батч-отбивки искать по тексту сообщения (`dispatch batch completed`) —
у неё нет `correlationId`.

**Общее правило:** сообщений в Kafka меньше или столько же, сколько
ожидалось, а результата нет/меньше → смотреть на **консьюмера**.
Сообщений больше одного на действие → смотреть на **продюсера**.
