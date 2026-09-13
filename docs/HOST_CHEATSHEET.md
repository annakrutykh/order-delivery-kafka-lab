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

Сразу после пуша — вернуться на страницу PR и показать:
- **Conversation** — появился новый коммит в списке (сверху над чеками), и **чеки снова зелёные** (были зелёными и на баге, и остались зелёными на фиксе — это и есть главный вывод: тесты не отреагировали ни на баг, ни на фикс, потому что вообще не смотрят в эту область).
- **Files changed** — теперь показывает суммарный диф PR (бажный коммит + фикс), либо кликнуть на новый коммит отдельно, чтобы увидеть именно fix-diff (он — зеркальное отражение бажного).

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

## Пошагово по каждому багу — цепочки команд

Каждый баг — **один блок**, вставляешь в терминал целиком (весь блок сразу,
не по одной строке), id заказа подхватывается сам через переменную `$ID`.
Работать в одном и том же окне терминала, не закрывать между командами —
иначе `$ID` потеряется.

Ветка `feature/producer-bugs` — пересобирать `order-api`.
Ветка `feature/consumer-bug` — пересобирать `delivery-worker`.

### Перед багами 1-3 — показать PR `feature/producer-bugs`

**Ссылка:** https://github.com/annakrutykh/order-delivery-kafka-lab/pull/1

Что показать студентам на этой странице, по вкладкам:
- **Conversation** (главная вкладка) — заголовок PR, описание, и внизу блок с чеками — обрати внимание: **все зелёные** (`order-api` и `delivery-worker` — два джоба CI).
- **Checks** — та же информация подробнее: клик на джоб `order-api` показывает лог линта и тестов, все шаги зелёные, ни один не упал.
- **Files changed** — реальный диф кода: изменения в `order-api/app/main.py`. Именно здесь физически лежат баги 1-3 — можно (но не обязательно) дать студентам самим повспоминать код и попробовать найти баг чтением, прежде чем тестировать руками.

Вывод для студентов на этом шаге: **пайплайн зелёный, код смёржился бы без вопросов** — а дальше идём проверять руками, работает ли это на самом деле.

### Баг 1 — дубль сообщения (обычный переход статуса)

```bash
ID=$(curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Test", "customer_address": "Test"}' | jq -r .id)
echo "заказ id: $ID"

curl -s -X PATCH http://localhost:8000/orders/$ID/status -H "Content-Type: application/json" \
  -d '{"status":"IN_PROGRESS"}' > /dev/null

curl -s -X PATCH http://localhost:8000/orders/$ID/status -H "Content-Type: application/json" \
  -d '{"status":"READY_FOR_DELIVERY"}' > /dev/null

echo "--- сообщений в Kafka для этого заказа (ожидание: 1, баг: 2) ---"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep "\"correlationId\": \"$ID\""
```

---

### Баг 2 — битое сообщение (переход напрямую, минуя `IN_PROGRESS`)

```bash
ID=$(curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Test", "customer_address": "Test"}' | jq -r .id)
echo "заказ id: $ID"

curl -s -X PATCH http://localhost:8000/orders/$ID/status -H "Content-Type: application/json" \
  -d '{"status":"READY_FOR_DELIVERY"}' > /dev/null

echo "--- статус доставки (ожидание: 200 с данными, баг: 404) ---"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/orders/$ID/delivery

echo "--- сообщение в Kafka (ожидание: есть orderId/occurredAt, баг: их нет) ---"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep "\"correlationId\": \"$ID\""

echo "--- ошибка в логах воркера ---"
docker compose logs delivery-worker --since 2m 2>&1 | grep "$ID"
```

---

### Баг 3 — неполный батч (`dispatch-batch`)

```bash
N=$(docker compose exec -T postgres psql -U orders_user -d orders_db -t \
  -c "SELECT count(*) FROM orders WHERE status='READY_FOR_DELIVERY';" | tr -d ' ')
echo "N (реально в БД): $N"

echo "--- ответ ручки (ожидание: dispatchedCount = N) ---"
curl -s -X POST http://localhost:8000/orders/dispatch-batch

echo ""
echo "--- реально отправлено сообщений (ожидание: = N, баг: N-1) ---"
docker compose logs order-api --since 1m 2>&1 | grep -c "dispatching order to"
```

---

### Перед багом 4 — показать PR `feature/consumer-bug`

**Ссылка:** https://github.com/annakrutykh/order-delivery-kafka-lab/pull/2

Те же три вкладки, что и для первого PR:
- **Conversation** — чеки внизу зелёные.
- **Checks** — оба джоба (`order-api`, `delivery-worker`) прошли.
- **Files changed** — диф в `delivery-worker/worker/main.py` и `worker/db.py`. Коммит называется нейтрально («reduce commit overhead с batched auto-commit») — специально звучит как безобидная оптимизация, а не как баг. Это тоже стоит проговорить студентам: заголовок коммита не гарантирует, что внутри нет проблемы.

### Баг 4 — дубль доставки после рестарта воркера

**Часть 1 — рестарт + заказ + проверка «до»** (весь блок сразу, не мешкая):

```bash
docker compose restart delivery-worker
sleep 5

ID=$(curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Test", "customer_address": "Test"}' | jq -r .id)
echo "заказ id: $ID"

curl -s -X PATCH http://localhost:8000/orders/$ID/status -H "Content-Type: application/json" \
  -d '{"status":"IN_PROGRESS"}' > /dev/null

curl -s -X PATCH http://localhost:8000/orders/$ID/status -H "Content-Type: application/json" \
  -d '{"status":"READY_FOR_DELIVERY"}' > /dev/null
sleep 3

echo "--- ДО рестарта: сообщений в Kafka (ожидание: 1) ---"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep -c "\"correlationId\": \"$ID\""

echo "--- ДО рестарта: записей в deliveries (ожидание: 1) ---"
docker compose exec -T postgres psql -U orders_user -d orders_db -t \
  -c "SELECT count(*) FROM deliveries WHERE order_id=$ID;"
```

**Часть 2 — рестарт «передеплоя»** (не позже ~1.5 минут после части 1, тот же терминал):

```bash
docker compose restart delivery-worker
```

Подождать 30–60 секунд, затем:

```bash
echo "--- ПОСЛЕ рестарта: сообщений в Kafka (ожидание: по-прежнему 1) ---"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic order.status_changed \
  --from-beginning --timeout-ms 8000 2>/dev/null | grep -c "\"correlationId\": \"$ID\""

echo "--- ПОСЛЕ рестарта: записей в deliveries (ожидание: 1, баг: 2) ---"
docker compose exec -T postgres psql -U orders_user -d orders_db -t \
  -c "SELECT count(*) FROM deliveries WHERE order_id=$ID;"
```
