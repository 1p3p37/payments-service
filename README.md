# Payments Service

Асинхронный сервис процессинга платежей: принимает платёж, обрабатывает его через эмулированный платёжный шлюз и уведомляет клиента webhook'ом. Гарантии — outbox, идемпотентность, retry с экспоненциальной задержкой и DLQ.

**Стек:** FastAPI · Pydantic v2 · SQLAlchemy 2 (async) · PostgreSQL · RabbitMQ (FastStream 0.7) · Alembic · dishka · Docker

## Запуск

```bash
docker compose up --build -d
docker compose ps          # migrate — Exited (0), остальные — running / healthy
```

| Сервис | Назначение | Снаружи |
|---|---|---|
| `postgres`, `rabbitmq` | хранилище и брокер | RabbitMQ UI — http://localhost:15672 (guest / guest) |
| `migrate` | `alembic upgrade head`, one-shot; остальные стартуют после его успеха | — |
| `api` | HTTP API, Swagger — http://localhost:8000/docs | `:8000` |
| `consumer` | обработка `payments.new` | — |
| `outbox-relay` | outbox → RabbitMQ | — |
| `webhook-mock` | демо-получатель webhook'ов (не часть сервиса) | `:8001` |

API-ключ по умолчанию — `dev-api-key` (переменная `API_KEY`). Порты меняются через `API_PORT`, `RABBITMQ_UI_PORT`, `WEBHOOK_MOCK_PORT`. Один образ на все сервисы, команды в exec-форме — `docker compose down` завершает процессы штатно за секунды.

## Примеры

Создать платёж (`webhook_url` — адрес mock'а внутри docker-сети):
```bash
curl -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: order-42' -H 'Content-Type: application/json' \
  -d '{"amount":"100.50","currency":"RUB","description":"Order #42","metadata":{"order_id":42},"webhook_url":"http://webhook-mock:8001/hook"}'
```
```
202 {"payment_id":"1a7a8ee2-c785-47a2-9fd3-9224124e8193","status":"pending","created_at":"2026-10-29T19:49:46.143151Z"}
```

| Запрос | Ответ |
|---|---|
| тот же запрос ещё раз | `202` — тот же `payment_id`, новой записи нет |
| тот же `Idempotency-Key`, другая сумма | `409 {"detail":"Idempotency key 'order-42' was already used with a different request body"}` |
| без `X-API-Key` | `401 {"detail":"Invalid or missing API key"}` |
| `amount: "0"`, `currency: "GBP"`, `webhook_url: "nope"` | `422` с перечнем ошибок полей |
| `GET` несуществующего платежа | `404 {"detail":"Payment … not found"}` |

Через 2–5 с платёж обработан:
```bash
curl localhost:8000/api/v1/payments/1a7a8ee2-c785-47a2-9fd3-9224124e8193 -H 'X-API-Key: dev-api-key'
```
```json
{"payment_id":"1a7a8ee2-…","amount":"100.50","currency":"RUB","description":"Order #42","metadata":{"order_id":42},
 "status":"succeeded","idempotency_key":"order-42","webhook_url":"http://webhook-mock:8001/hook",
 "created_at":"2026-10-29T19:49:46.143151Z","processed_at":"2026-10-29T19:49:48.864445Z"}
```
а `curl localhost:8001/received` показывает доставленный webhook с `X-Event-Id` = `payment_id`.

### Retry и DLQ вживую

```bash
curl -X PUT localhost:8001/config -H 'Content-Type: application/json' -d '{"fail_rate":1}'   # webhook всегда 500
# создать платёж, затем:
docker compose logs -f consumer
```
```
19:50:29.237 WARNING app.consumer.retry Processing failed, message 685fee34-… → retry 1/3
19:50:30.270 WARNING app.consumer.retry Processing failed, message 685fee34-… → retry 2/3   (+1 с)
19:50:32.330 WARNING app.consumer.retry Processing failed, message 685fee34-… → retry 3/3   (+2 с)
19:50:36.355 ERROR   app.consumer.retry Retries exhausted (3), message 685fee34-… → DLQ  (+4 с)
```
Сообщение — в очереди `payments.new.dlq` (RabbitMQ UI → Queues). Вернуть: `-d '{"fail_rate":0}'`.

### Масштабирование

```bash
docker compose up -d --scale consumer=3
```
Реплики делят очередь `payments.new`; конфликты при дублях закрывает optimistic lock. Проверено: 30 параллельных платежей распределились по трём consumer'ам, все обработаны, outbox пуст.

## Архитектура

```
 POST /api/v1/payments
        │
        ▼
 ┌─────────────┐   одна транзакция   ┌──────────────────────┐
 │     API     │ ──────────────────► │ payments  +  outbox  │  PostgreSQL
 └─────────────┘                     └──────────┬───────────┘
                                                │ FOR UPDATE SKIP LOCKED
                                     ┌──────────▼───────────┐
                                     │    outbox relay      │  publish (confirm) → delete
                                     └──────────┬───────────┘
                                                ▼
                         [payments] ──payments.new──► payments.new
                              ▲                            │
                   TTL истёк  │                            ▼
                              │                     ┌─────────────┐   шлюз (2–5 с, 90 %)
    payments.new.retry.1/2/3 ◄┴── [payments.retry] ◄│  consumer   │──► статус в БД
          (1 с / 2 с / 4 с)          ошибка webhook  └──────┬──────┘──► webhook клиенту
                                                           │ попытки исчерпаны
                                                           ▼
                                     [payments.dlx] ──► payments.new.dlq
```

### Слои

| Слой | Что внутри |
|---|---|
| `domain/` | `Payment` с правилами переходов статусов, доменные события, исключения. Без зависимостей от инфраструктуры |
| `application/` | Команды и запросы (CQRS), хендлеры, минимальный медиатор |
| `infra/` | ORM-модели, query/command-репозитории, UoW, топология RabbitMQ, шлюз, webhook-клиент |
| `ioc.py` | DI-контейнер (dishka) — одна сборка для API, consumer, relay и тестов |
| `api/` | FastAPI: роуты, схемы, проверка `X-API-Key`, маппинг доменных ошибок в HTTP |
| `consumer/` | FastStream-подписчик и retry-middleware |
| `manage.py` | CLI: `runoutbox` |

### Поток платежа

1. **API** в одной транзакции сохраняет платёж (`pending`) и событие `payments.new` в таблицу `outbox`. Платёж без события или событие без платежа невозможны.
2. **Outbox relay** забирает пачку событий (`FOR UPDATE SKIP LOCKED`), публикует в RabbitMQ и удаляет строки, которые брокер подтвердил.
3. **Consumer** получает сообщение:
   ```python
   async with uow:
       payment = await mediator.query(GetPaymentQuery(payment_id))
   if payment.is_pending:
       succeeded = await mediator.send(ChargePaymentCommand(...))      # шлюз — вне транзакции
       async with uow:
           payment = await mediator.send(CompletePaymentCommand(payment.id, succeeded))
   await mediator.send(SendWebhookCommand(payment))
   ```
   Шлюз вызывается **между** транзакциями: держать соединение с БД 2–5 секунд нельзя (исчерпание пула), а откат транзакции всё равно не отменил бы списание.
4. Ошибка отправки webhook → сообщение уходит в retry-очередь и через 1 / 2 / 4 с возвращается; после исчерпания попыток — в `payments.new.dlq`.

## Гарантии доставки

**At-least-once на всём пути.** Опубликовать в RabbitMQ и удалить из Postgres атомарно нельзя, поэтому порядок «сначала publish, потом delete»: при сбое между ними событие уйдёт повторно, но не потеряется. Сообщение может прийти дважды и от RabbitMQ (crash consumer'а до ack). Поэтому каждый шаг идемпотентен:

| Повтор | Почему безопасен |
|---|---|
| Повторная доставка обработанного платежа | `is_pending == False` → шлюз не вызывается, только webhook |
| Два consumer'а получили одно сообщение | optimistic lock (`version`): второй `UPDATE` получает 0 строк → перечитывает и возвращает результат первого |
| Повторный вызов шлюза | шлюз идемпотентен по `payment_id` (как ключ идемпотентности у реальных шлюзов) |
| Повторный webhook | заголовок `X-Event-Id` — получатель отсекает дубли |

**Публикация надёжна только при трёх условиях** — все соблюдены:
- `persist=True` — persistent-сообщение в durable-очереди переживает рестарт RabbitMQ;
- publisher confirms — `publish` завершается только после подтверждения брокера;
- `on_return_raises=True` — сообщение без маршрута даёт ошибку. Без этого флага FastStream считает такую публикацию успешной (проверено на RabbitMQ), и relay удалил бы событие.

## Идемпотентность API

- `Idempotency-Key` уникален в БД. Повтор с тем же ключом и телом возвращает существующий платёж.
- Тот же ключ с **другим** телом → `409 Conflict`. Тело сравнивается по blake2b-хэшу канонического JSON; сумма предварительно округляется, поэтому `100`, `100.0` и `100.00` — один запрос.
- Гонка двух одинаковых запросов: `INSERT … ON CONFLICT DO NOTHING` ждёт коммита соседа, проигравший перечитывает его запись — в БД одна строка.

## Retry и DLQ

- **«3 попытки»** = исходная обработка + 3 повтора с задержками 1 / 2 / 4 с, затем DLQ. Настраивается `MAX_RETRIES` и `RETRY_BASE_DELAY`.
- **Своя очередь на каждую попытку** с TTL на уровне очереди: нет head-of-line блокировки, как при per-message `expiration` в общей очереди; consumer не блокируется на время задержки.
- **Ошибка шлюза (10 %) не ретраится.** Это бизнес-результат: платёж получает `failed`, клиент — webhook. Ретраятся только технические сбои (webhook, БД, сеть). ТЗ требует повторов «при ошибках отправки».
- **Отсутствующий платёж** — сразу в DLQ: повтор не поможет.

## Consumer

```bash
faststream run app.consumer.main:create_app --factory
```
- Политика повторов — FastStream middleware (`app/consumer/retry.py`), хендлер остаётся чистым сценарием. Ack-механизм FastStream стоит снаружи пользовательских middleware, поэтому `RejectMessage` из неё уходит в DLX.
- Сразу в DLQ, без повторов: несуществующий платёж и сообщение, не прошедшее валидацию.
- Не удалось отложить сообщение в retry-очередь (брокер недоступен) → `nack` с возвратом в очередь, а не досрочный DLQ.
- Горизонтальное масштабирование: несколько реплик на одной очереди, `prefetch_count=10` на каждую.

## Outbox relay

```bash
python -m app.manage runoutbox
```
- Пачки по `OUTBOX_BATCH_SIZE`, параллельная публикация до `OUTBOX_PUBLISH_CONCURRENCY`.
- Несколько реплик делят работу благодаря `SKIP LOCKED`.
- Пауза `OUTBOX_POLL_INTERVAL`, только когда продвинуться не удалось — нет горячего цикла при недоступном брокере.
- **Poison-сообщения:** если брокер отклоняет конкретное сообщение (нет маршрута, nack), растёт `outbox.attempts`; после `OUTBOX_MAX_ATTEMPTS` строка больше не выбирается и не блокирует новые события, но остаётся в таблице с `last_error` для разбора. Недоступность брокера попытки **не** расходует — иначе простой отправил бы в карантин весь outbox.
- **Graceful shutdown:** по SIGTERM текущая пачка доводится до конца, ресурсы закрываются. Без обработчика Python на SIGTERM завершается без `finally`, а как PID 1 в контейнере сигнал игнорируется до SIGKILL.

## Принятые решения

| Решение | Почему |
|---|---|
| **DI на dishka** | Три точки входа (API, consumer, relay) собирают один граф зависимостей. Async-провайдеры с финализацией закрывают engine и HTTP-клиент; REQUEST-скоуп даёт одну сессию на запрос/сообщение |
| **UoW — только граница транзакции**, без ContextVar | Сессия — явная REQUEST-зависимость контейнера, её получают и UoW, и репозитории. Транзакцию открывает точка входа, хендлеры о транзакциях не знают |
| **CQRS на уровне репозиториев** | На сущность — query- и command-репозиторий на одной сессии; модель одна (`to_entity()`), отдельных read-моделей нет — один источник правды |
| **Медиатор — минимальный реестр** | Явный словарь `тип → хендлер`, хендлеры из request-контейнера. Регистрация декораторами, автоскан и pipeline behaviors — точки роста, но при пяти хендлерах словарь нагляднее. Behaviors для транзакций не подходят: consumer'у нужны две транзакции |
| **Логика в командах, порядок шагов в точке входа** | Consumer — 7 строк сценария; решения («уже обработан?», «сосед успел раньше?») в домене и хендлерах; каждая команда тестируется отдельно |
| **Отправленные события удаляются**, а не помечаются | Таблица outbox остаётся маленькой, не нужны индекс неотправленных и cron-очистка |
| **Optimistic lock** вместо блокировок | Конфликт редкий (дубль сообщения), ожидание шлюза не держит блокировку |

## Как было бы с реальным шлюзом

Эмуляция в ТЗ — один вызов с ответом через 2–5 с. Реальная интеграция обычно устроена иначе:
- POST в шлюз с ключом идемпотентности (`payment_id`) быстро отвечает «принято»; итог приходит позже **webhook'ом шлюза**.
- Webhook шлюза — только сигнал: он может потеряться, задублироваться или быть подделан, поэтому статус подтверждают `GET` в шлюз.
- Перед вызовом шлюза записывается намерение — статус `processing`; при повторе с `processing` шлюз не вызывают снова, а спрашивают статус.
- Периодическая **сверка** (reconciliation) опрашивает платежи, зависшие в `processing`.

## Известные ограничения

- `/health` открыт без `X-API-Key` — нужен для healthcheck в compose.
- Смена `MAX_RETRIES` / `RETRY_BASE_DELAY` требует удалить retry-очереди: RabbitMQ не даёт переобъявить очередь с другими аргументами.
- Сообщения в карантине outbox переотправляются вручную (`UPDATE outbox SET attempts = 0 WHERE …`).

## Переменные окружения

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DB_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | PostgreSQL |
| `RABBIT_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |
| `API_KEY` | `dev-api-key` | значение `X-API-Key` |
| `MAX_RETRIES` | `3` | повторов перед DLQ |
| `RETRY_BASE_DELAY` | `1` | секунды; задержка попытки n = base · 2^(n−1) |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5` | секунды ответа шлюза |
| `WEBHOOK_TIMEOUT` | `5` | секунды |
| `OUTBOX_BATCH_SIZE` | `1000` | событий за итерацию relay |
| `OUTBOX_PUBLISH_CONCURRENCY` | `100` | параллельных публикаций |
| `OUTBOX_MAX_ATTEMPTS` | `5` | отказов брокера до карантина |
| `OUTBOX_POLL_INTERVAL` | `0.5` | секунды паузы relay |
| `OUTBOX_SHUTDOWN_TIMEOUT` | `30` | секунды на закрытие при остановке |

## Разработка и тесты

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest                 # unit + integration; e2e пропускаются, если стек не поднят
docker compose up -d && uv run pytest -m e2e    # e2e на поднятом окружении
```

Integration-тесты поднимают **PostgreSQL и RabbitMQ в testcontainers** (нужен Docker) и применяют настоящие миграции. Retry-задержки и DLQ проверяются на реальном брокере: `TestRabbitBroker` не эмулирует TTL и dead-lettering.

| Набор | Что проверяет |
|---|---|
| unit | правила домена, медиатор, шлюз, webhook-клиент, параметры retry-очередей |
| integration | репозитории и optimistic lock, атомарность UoW, миграции = модели, идемпотентность и гонки создания, конфликт параллельных consumer'ов, TTL → возврат в очередь, reject → DLQ, outbox: persistent-публикация, карантин poison-сообщений, поведение при недоступном брокере; consumer по полному пути create → outbox → relay → очередь: успех, сбой webhook → retry, исчерпание → DLQ с задержками 1/2/4·base, неизвестный платёж и битое тело → DLQ без повторов, повторная доставка без второго списания |
| integration: API | 401 (нет / неверный ключ, раньше валидации), 202, повтор → тот же ответ, 409, 5 одновременных POST → одна запись, 422 на невалидное тело и отсутствующий `Idempotency-Key`, GET 200 / 404 / 422 |
| e2e (`-m e2e`) | на docker compose: POST → `succeeded/failed` → webhook-mock получил уведомление; webhook всегда 500 → сообщение в `payments.new.dlq` (management API) |
