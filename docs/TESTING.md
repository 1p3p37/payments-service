# Тестирование Payments Service

Как проверить сервис: автотесты, ручные сценарии через curl / Postman и сценарии отказов. Устройство сервиса — в [ARCHITECTURE](ARCHITECTURE.md).

## 1. Автотесты

```bash
uv sync
uv run ruff check . && uv run ruff format --check .   # + ASYNC, S (bandit), DTZ, PT
uv run mypy                      # strict; в tests/ ослаблены только требования к аннотациям
uv run pytest                    # unit + integration; e2e пропускаются без поднятого стека
docker compose up -d && uv run pytest -m e2e
```

| Набор | Где | Окружение | Время |
|---|---|---|---|
| unit | `tests/unit/` | ничего не нужно | < 1 с |
| integration | `tests/integration/` | Docker: testcontainers поднимает PostgreSQL 16 и RabbitMQ 3.13 | ~20 с |
| e2e | `tests/e2e/` | запущенный `docker compose` | ~25 с |

Всего 106 тестов. Integration-тесты помечаются маркером `integration` автоматически (по каталогу), e2e — маркером `e2e`; без Docker integration и без стека e2e пропускаются с причиной в `pytest -rs`.

### Почему integration на настоящем RabbitMQ

`TestRabbitBroker` FastStream подменяет брокер в памяти и **не эмулирует** TTL, dead-lettering и publisher confirms. Именно на них держатся retry, DLQ и надёжность outbox, поэтому топология, relay и consumer проверяются на реальном брокере в контейнере. Миграции применяются настоящие (`alembic upgrade head`), а не `create_all`.

### Что покрыто

| Файл | Проверяет |
|---|---|
| `unit/test_payment.py` | создание и валидация суммы, округление, событие `payments.new`, переходы статусов, стабильность хэша запроса |
| `unit/test_models.py` | `Payment.to_dict()` ↔ колонки таблицы, round-trip `to_entity()` |
| `unit/test_mediator.py` | диспетчеризация через настоящий dishka-контейнер, незарегистрированный запрос |
| `unit/test_gateway.py` | идемпотентность шлюза по `payment_id`, доля успехов ≈ 90 % на 2000 id |
| `unit/test_webhook.py` | тело и `X-Event-Id`, 500 и сетевая ошибка → `WebhookDeliveryError` (respx) |
| `unit/test_broker.py`, `unit/test_retry.py` | TTL 1/2/4 с, аргументы retry-очередей, `decide(attempt, max)` |
| `integration/test_repositories.py` | round-trip, дубль `idempotency_key`, optimistic lock, перечитывание после конфликта, SKIP LOCKED, карантин outbox, атомарность UoW |
| `integration/test_migrations.py` | миграции совпадают с моделями (`compare_metadata`) |
| `integration/test_application.py` | создание / повтор / 409 / 5 одновременных запросов, complete: повтор и победа соседа, сценарий consumer'а без брокера |
| `integration/test_topology.py` | TTL → возврат в `payments.new`, reject → DLQ, повторный declare |
| `integration/test_outbox_relay.py` | persistent-публикация и удаление, poison → карантин без блокировки, брокер недоступен → `attempts` не растёт, остановка по сигналу |
| `integration/test_consumer.py` | полный путь create → outbox → relay → очередь: успех, webhook падает 1 раз / всегда (задержки и DLQ с `x-retry-count=3`), неизвестный платёж и битое тело → DLQ без retry, повторная доставка без второго списания |
| `integration/test_api.py` | ключ на всех эндпоинтах (включая `/health`), 401 раньше 422, Swagger открыт, но все операции в схеме требуют ключ, выключение флагом, 202, повтор, 409, гонка, 422 на невалидное тело, GET 200 / 404 / 422 |
| `e2e/test_flow.py` | POST → `succeeded/failed` → webhook-mock получил уведомление; webhook всегда 500 → сообщение в DLQ |

### Инфраструктура тестов

- `tests/integration/conftest.py`:
  - `db_url`, `rabbit_url` — контейнеры на сессию; `engine` — `TRUNCATE` перед каждым тестом;
  - `scope` / `open_scope` — то, что DI собирает на один запрос (одна сессия, UoW, репозитории); два scope = два параллельных клиента;
  - `container`, `relay_container` — DI-контейнер приложения; шлюз подменён мгновенным (`InstantGatewayProvider`);
  - `broker` — топология объявлена, очереди очищены; `consumer` — запущенный consumer (`TestApp` на реальном брокере) со шлюзом-счётчиком `CountingGateway`.
- `tests/integration/helpers.py` — `send()` (вызов как из точки входа: свой скоуп и транзакция), `wait_until()` (опрос вместо `sleep`).
- Retry-задержки в тестах — `RETRY_BASE_DELAY=0.2`, чтобы сценарий DLQ шёл ~1.5 с.

### Как проверялись сами тесты

Выборочно — с намеренно сломанным кодом: например, без `ValidationError` в non-retryable тест битого сообщения падает (в заголовках видно, что сообщение прошло через retry-очередь). Тест, который не падает на сломанном коде, ничего не проверяет.

## 2. Ручная проверка

```bash
docker compose up --build -d
docker compose ps        # migrate — Exited (0); api, postgres, rabbitmq — healthy
```

Адреса: API — `http://localhost:8000` (Swagger `/docs` открывается без ключа; для запросов из него — **Authorize** → `dev-api-key`, иначе ручки ответят 401), webhook-mock — `http://localhost:8001`, RabbitMQ UI — `http://localhost:15672` (guest / guest). Ключ — `dev-api-key`.

**Postman:** переменные `base_url = http://localhost:8000`, `api_key = dev-api-key`; заголовок `X-API-Key: {{api_key}}` на коллекцию; в Tests запроса создания — `pm.collectionVariables.set("payment_id", pm.response.json().payment_id)`.

> `webhook_url` в запросах — `http://webhook-mock:8001/hook`: webhook отправляет consumer **изнутри docker-сети**, `localhost` там — сам контейнер.

### 2.1 Основной сценарий

| # | Запрос | Ожидается |
|---|---|---|
| 1 | `GET /health` с ключом / без ключа | `200 {"status":"ok"}` / `401` — ключ нужен на всех эндпоинтах |
| 2 | создать платёж (ниже) | `202 {payment_id, status:"pending", created_at}` |
| 3 | повторить п. 2 без изменений | `202`, тот же `payment_id` и `created_at` |
| 4 | п. 2 с тем же ключом и `"amount":"999.00"` | `409 already used with a different request body` |
| 5 | `GET /api/v1/payments/{payment_id}` сразу | `status: "pending"`, `processed_at: null` |
| 6 | то же через 2–5 с | `succeeded` (≈ 90 %) или `failed`, `processed_at` заполнен |
| 7 | `GET localhost:8001/received` | уведомление с `event_id = payment_id` и тем же статусом |

```bash
curl -i -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: order-42' -H 'Content-Type: application/json' \
  -d '{"amount":"100.50","currency":"RUB","description":"Order #42","metadata":{"order_id":42},"webhook_url":"http://webhook-mock:8001/hook"}'

curl -i localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'
curl localhost:8001/received
```

### 2.2 Ошибки клиента

| Запрос | Ожидается |
|---|---|
| без `X-API-Key` или с неверным | `401 Invalid or missing API key` (и раньше, чем ошибки валидации) |
| без `Idempotency-Key` | `422` |
| `"amount":"0"`, `"-1"` или `"10.001"` | `422` |
| `"currency":"GBP"` | `422` (допустимы RUB / USD / EUR) |
| `"webhook_url":"nope"` | `422` |
| `description` длиннее 255 | `422` |
| `GET` несуществующего UUID | `404 Payment … not found` |
| `GET /api/v1/payments/not-a-uuid` | `422` |

```bash
curl -i -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: bad-1' -H 'Content-Type: application/json' \
  -d '{"amount":"0","currency":"GBP","description":"x","webhook_url":"nope"}'
```

### 2.3 Идемпотентность под нагрузкой

10 одинаковых запросов параллельно → одна запись:
```bash
for i in $(seq 1 10); do
  curl -s -o /dev/null -w '%{http_code} ' -X POST localhost:8000/api/v1/payments \
    -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: race-1' -H 'Content-Type: application/json' \
    -d '{"amount":"5.00","currency":"EUR","description":"race","webhook_url":"http://webhook-mock:8001/hook"}' &
done; wait; echo
docker compose exec postgres psql -U payments -c "select count(*) from payments where idempotency_key='race-1'"
```
Ожидается: десять `202` и `count = 1`.

## 3. Сценарии отказов

### 3.1 Webhook недоступен → retry → DLQ

```bash
curl -X PUT localhost:8001/config -H 'Content-Type: application/json' -d '{"fail_rate":1}'
# создать платёж (п. 2.1 с новым Idempotency-Key)
docker compose logs -f consumer
```
Ожидается в логах: `retry 1/3`, через ~1 с `retry 2/3`, через ~2 с `retry 3/3`, через ~4 с `Retries exhausted (3) … → DLQ`. Сообщение — в очереди `payments.new.dlq`:
```bash
curl -s -u guest:guest localhost:15672/api/queues/%2F/payments.new.dlq | python3 -c 'import sys,json;print(json.load(sys.stdin)["messages"])'
```
Статус платежа при этом уже `succeeded` / `failed` — сохраняется до webhook'а. В UI: Queues → `payments.new.dlq` → Get messages — видно заголовки `x-retry-count: 3` и `x-death`.

Частичный сбой: `{"fail_rate":0.5}` — часть webhook'ов доходит со второй-третьей попытки, DLQ не растёт.

Вернуть: `curl -X PUT localhost:8001/config -H 'Content-Type: application/json' -d '{"fail_rate":0}'`.

### 3.2 Consumer остановлен

```bash
docker compose stop consumer
# создать несколько платежей — 202, статус остаётся pending
curl -s -u guest:guest localhost:15672/api/queues/%2F/payments.new | python3 -c 'import sys,json;print(json.load(sys.stdin)["messages"])'
docker compose start consumer
```
Ожидается: пока consumer стоит, сообщения копятся в `payments.new`, outbox пуст (relay их уже опубликовал); после старта все платежи переходят в `succeeded` / `failed`.

### 3.3 RabbitMQ недоступен

```bash
docker compose stop rabbitmq
# создать платёж — API отвечает 202
docker compose exec postgres psql -U payments -c "select id, attempts, last_error from outbox"
docker compose logs --tail 5 outbox-relay
docker compose start rabbitmq
```
Ожидается: событие лежит в outbox с `attempts = 0` (недоступность брокера попытки не расходует); relay логирует ошибки с паузой, а не непрерывным потоком; после старта RabbitMQ relay публикует событие, consumer переподключается и обрабатывает платёж, outbox пуст.

### 3.4 Повторная доставка

Опубликовать уже обработанный платёж ещё раз (UI → Exchanges → `payments` → Publish message, routing key `payments.new`, payload `{"payment_id":"<id>"}`). Ожидается: статус и `version` не меняются (шлюз повторно не вызывается), webhook-mock получает уведомление ещё раз с пометкой `duplicate: true`.

### 3.5 Битое сообщение

Publish в `payments` с routing key `payments.new` и payload `{"foo":1}` или `{"payment_id":"<случайный uuid>"}`. Ожидается: сообщение сразу в `payments.new.dlq`, без `x-retry-count` — повтор не помог бы.

### 3.6 Масштабирование и остановка

```bash
docker compose up -d --scale consumer=3
# серия платежей — обрабатываются всеми репликами
for c in 1 2 3; do docker logs payments-service-consumer-$c 2>&1 | grep -c 'processed:'; done
time docker compose down
```
Ожидается: обработки распределены по репликам, outbox пуст; `docker compose down` занимает секунды, а не 10 с на сервис (процессы получают SIGTERM и завершаются штатно).

## 4. Диагностика

| Симптом | Где смотреть |
|---|---|
| платёж навсегда `pending` | `docker compose logs consumer outbox-relay`; глубина `payments.new`; строки в `outbox` |
| событие не уходит из outbox | `select attempts, last_error from outbox` — `attempts ≥ OUTBOX_MAX_ATTEMPTS` значит карантин (брокер отверг сообщение) |
| `PRECONDITION_FAILED` при старте consumer / relay | сменились `MAX_RETRIES` / `RETRY_BASE_DELAY` — удалить retry-очереди в UI или `docker compose down -v` |
| webhook не приходит | `curl localhost:8001/received`, `fail_rate` mock'а, `webhook_url` должен указывать на `webhook-mock`, не `localhost` |
| нужно начать с чистого состояния | `docker compose down -v` (удаляет volumes Postgres и RabbitMQ) |
