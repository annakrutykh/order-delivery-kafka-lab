# Шпаргалка ведущего — команды и доступы

Короткая памятка для занятия: ссылки на сервисы, логины, и команды —
отдельно для ведущего («разработчик») и для того, что делают студенты
(«тестировщик»). Полные версии — `docs/LESSON_PLAN.md` (план занятия),
`docs/TESTER_TASKS.md` (задачи для тестировщика), `docs/KAFKA_TESTING_THEORY.md`
(теория).

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

**Kibana data view:** имя `test` (или как назвала), index pattern `app-logs-*`, time field `@timestamp`.

---

## 🔧 Ведущий («разработчик»)

### Перед занятием — проверить, что стенд поднят
```bash
docker compose ps
```
Все сервисы — `Up` (kafka и postgres — ещё и `healthy`).

### Цикл «показать баг → фикс» (одинаково для обеих веток)

**1. Показать PR и CI на GitHub** → вкладка **Checks** → все зелёные.

**2. Подтянуть ветку с багом и пересобрать:**
```bash
git fetch origin
git checkout feature/producer-bugs        # или feature/consumer-bug
git pull
git log --oneline -5
docker compose up --build -d order-api    # order-api для producer-bugs
docker compose up --build -d delivery-worker   # delivery-worker для consumer-bug
```

**3. После того как студенты нашли баг(и) — запушить fix и подтянуть:**
```bash
git push origin feature/producer-bugs     # или feature/consumer-bug
git pull
docker compose up --build -d order-api    # или delivery-worker
```

**4. Только для `consumer-bug` — рестарт воркера (имитация передеплоя):**
```bash
docker compose restart delivery-worker
```
Делать рестарт **в течение ~1.5 минут** после обработки заказа консьюмером
(интервал автокоммита в бажной версии — 2 минуты; позже этого окна баг не
воспроизведётся, т.к. оффсет уже успеет закоммититься).

### Если что-то пошло не так
```bash
docker compose down -v && docker compose up --build -d   # полный сброс стенда
```

---

## 👩‍💻 Студент («тестировщик»)

### Запросы в Swagger (`.../docs`)

| Ручка | Тело запроса |
|---|---|
| `POST /orders` | `{"customer_name": "...", "customer_address": "..."}` |
| `PATCH /orders/{id}/status` | `{"status": "IN_PROGRESS"}` или `{"status": "READY_FOR_DELIVERY"}` |
| `GET /orders/{id}/delivery` | — |
| `POST /orders/dispatch-batch` | — |

### Где смотреть результат

**Kafka UI** → топик → Messages → поле Search:
```
correlationId": "<ID>"
```

**Kibana** → Discover → дата-вью → KQL-фильтр:
```
correlationId: "<ID>"
```
Добавить колонку `name` (различает `order-api` / `delivery-worker`). Для
батч-отбивки искать по тексту сообщения (`dispatch batch completed`), у
неё нет `correlationId`.

**DBeaver:**
```sql
SELECT count(*) FROM orders WHERE status = 'READY_FOR_DELIVERY';
SELECT * FROM deliveries WHERE order_id = <ID>;
```

### Общее правило диагностики

Сообщений в Kafka **меньше или столько же**, сколько ожидалось, а
результата нет/меньше → смотреть на **консьюмера** (`delivery-worker`).
Сообщений **больше одного** на одно действие → смотреть на **продюсера**
(`order-api`).

---

## Пошагово по каждому багу

Ветка `feature/producer-bugs` — пересобирать `order-api`.
Ветка `feature/consumer-bug` — пересобирать `delivery-worker`.

### Баг 1 — дубль сообщения (обычный переход статуса)

**Swagger, по порядку:**
1. `POST /orders` — `{"customer_name": "Test", "customer_address": "Test"}` → запомнить `id`
2. `PATCH /orders/{id}/status` — `{"status": "IN_PROGRESS"}`
3. `PATCH /orders/{id}/status` — `{"status": "READY_FOR_DELIVERY"}`

**Проверка (Kafka UI → топик `order.status_changed` → Search):**
```
correlationId": "<id>"
```
**Ожидание:** 1 сообщение. **Баг:** 2 одинаковых сообщения.

---

### Баг 2 — битое сообщение (переход напрямую, минуя `IN_PROGRESS`)

**Swagger, по порядку:**
1. `POST /orders` — `{"customer_name": "Test", "customer_address": "Test"}` → запомнить `id`
2. `PATCH /orders/{id}/status` — сразу `{"status": "READY_FOR_DELIVERY"}` (без шага `IN_PROGRESS`)
3. `GET /orders/{id}/delivery`

**Проверка:**
- Ответ шага 3 — **ожидание:** `200` с данными. **Баг:** `404`.
- Kafka UI → топик `order.status_changed` → Search `correlationId": "<id>"` — **ожидание:** сообщение со всеми полями (`orderId`, `occurredAt`). **Баг:** этих полей нет.
- Kibana → фильтр `correlationId: "<id>"` — искать строку `levelname: ERROR`, `message: failed to process message` от `delivery-worker`.

---

### Баг 3 — неполный батч (`dispatch-batch`)

**DBeaver, до вызова ручки:**
```sql
SELECT count(*) FROM orders WHERE status = 'READY_FOR_DELIVERY';
```
Запомнить число — это **N**.

**Swagger:**
```
POST /orders/dispatch-batch
```
Посмотреть `dispatchedCount` в ответе.

**Проверка (Kafka UI → топик `order.dispatch_requested`, Newest First):**
Посчитать реальное число новых сообщений от этого вызова.

**Ожидание:** `dispatchedCount` = N = число сообщений в Kafka.
**Баг:** `dispatchedCount` = N (совпадает с БД), но сообщений в Kafka — N−1.

---

### Баг 4 — дубль доставки после рестарта воркера

Нужен свежий рестарт `delivery-worker` перед началом (сбрасывает таймер автокоммита):
```bash
docker compose restart delivery-worker
```

**Сразу после рестарта (в Swagger, по порядку, быстро):**
1. `POST /orders` — `{"customer_name": "Test", "customer_address": "Test"}` → запомнить `id`
2. `PATCH /orders/{id}/status` — `{"status": "IN_PROGRESS"}`
3. `PATCH /orders/{id}/status` — `{"status": "READY_FOR_DELIVERY"}`

**Проверка до рестарта:**
- Kafka UI → `order.status_changed` → Search `correlationId": "<id>"` — 1 сообщение
- DBeaver: `SELECT * FROM deliveries WHERE order_id = <id>;` — 1 запись

**Рестарт (не позже ~1.5 минут после обработки):**
```bash
docker compose restart delivery-worker
```
Подождать 30-60 секунд.

**Проверка после рестарта:**
- Kafka UI → то же сообщение — **ожидание:** по-прежнему 1. Не меняется.
- DBeaver: тот же запрос — **ожидание:** 1 запись. **Баг:** 2 записи.
