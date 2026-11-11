# Руководство по настройке прокси-сервера (`as_proxy_server`)

[🇬🇧 English version](../en/proxy-configuration.md) | 🇷🇺 **Русский** | [⬅ Назад к README](../../README.ru.md)

---

## Оглавление

1. [Порядок загрузки конфигурации](#1-порядок-загрузки-конфигурации)
2. [Флаги командной строки (CLI) и переменные окружения](#2-флаги-командной-строки-cli-и-переменные-окружения)
3. [Подробный справочник параметров `proxy.toml`](#3-подробный-справочник-параметров-proxytoml)
   - [`[server]` — Arrow Flight, HTTP/2 и компрессия](#31-server--arrow-flight-http2-и-компрессия)
   - [`[raft]` — Кластерный консенсус и TTL статуса джобов](#32-raft--кластерный-консенсус-и-ttl-статуса-джобов)
   - [`[tls]` — Шифрование TLS и взаимный TLS (mTLS)](#33-tls--шифрование-tls-и-взаимный-tls-mtls)
   - [`[auth]` — Аутентификация Handshake и Bearer-токены](#34-auth--аутентификация-handshake-и-bearer-токены)
   - [`[aerospike]` — Пул соединений к кластеру Aerospike](#35-aerospike--пул-соединений-к-кластеру-aerospike)
   - [`[writer]` — MPP-батчинг и параллелизм](#36-writer--mpp-батчинг-и-параллелизм)
   - [`[retry]` — Политика ретраев и экспоненциальный Backoff](#37-retry--политика-ретраев-и-экспоненциальный-backoff)
4. [Пример запуска промышленного кластера из 3 нод](#4-пример-запуска-промышленного-кластера-из-3-нод)
5. [Рекомендации по тюнингу производительности](#5-рекомендации-по-тюнингу-производительности)

---

## 1. Порядок загрузки конфигурации

`as_proxy_server` применяет настройки в следующем порядке приоритета (от высшего к низшему):
1. **Аргументы командной строки (CLI)** (например, `--listen-addr`, `--aerospike-hosts`)
2. **Переменные окружения** (например, `AS_PROXY_LISTEN_ADDR`, `AEROSPIKE_HOSTS`)
3. **Конфигурационный файл TOML** (`--config /path/to/proxy.toml` или `AS_PROXY_CONFIG`)
4. **Встроенные значения по умолчанию** (`AppConfig::default()`)

---

## 2. Флаги командной строки (CLI) и переменные окружения

| Флаг CLI | Переменная окружения | Описание | Пример |
| :--- | :--- | :--- | :--- |
| `-c`, `--config <PATH>` | `AS_PROXY_CONFIG` | Путь к файлу конфигурации `.toml` | `--config /etc/as_proxy/proxy.toml` |
| `--listen-addr <ADDR>` | `AS_PROXY_LISTEN_ADDR` | Переопределяет `server.listen_addr` (адрес Arrow Flight gRPC) | `0.0.0.0:8815` |
| `--raft-addr <ADDR>` | `AS_PROXY_RAFT_ADDR` | Переопределяет `raft.listen_addr` (адрес порта Raft) | `0.0.0.0:8816` |
| `--node-id <ID>` | `AS_PROXY_NODE_ID` | Переопределяет `raft.node_id` (уникальный ID ноды `>= 1`) | `1` |
| `--aerospike-hosts <HOSTS>` | `AEROSPIKE_HOSTS` | Переопределяет `aerospike.hosts` (список `host:port` через запятую) | `10.0.1.10:3000,10.0.1.11:3000` |
| *(логирование)* | `RUST_LOG` | Уровень логирования (`error`, `warn`, `info`, `debug`, `trace`) | `RUST_LOG=info,as_proxy_server=debug` |

---

## 3. Подробный справочник параметров `proxy.toml`

### 3.1. `[server]` — Arrow Flight, HTTP/2 и компрессия

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `listen_addr` | `String` | `"0.0.0.0:8815"` | Адрес и порт Arrow Flight gRPC сервера |
| `metrics_addr` | `String` | `"0.0.0.0:9090"` | Адрес и порт HTTP-сервера наблюдаемости (`/metrics` и `/healthz`) |
| `max_decoding_message_size_mb` | `usize` | `64` | Максимальный размер входящего gRPC-сообщения (МБ) |
| `tcp_nodelay` | `bool` | `true` | Включает `TCP_NODELAY` (отключает алгоритм Нейгла) для минимизации задержек |
| `http2_stream_window_size_mb` | `u32` | `16` | Размер окна управления потоком HTTP/2 на один стрим (МБ) |
| `http2_connection_window_size_mb` | `u32` | `32` | Размер окна управления потоком HTTP/2 на соединение (МБ) |
| `compression_enabled` | `bool` | `true` | **Мастер-переключатель** компрессии Arrow Flight. При `false` полностью отключает сжатие IPC и gRPC независимо от настроек клиента |
| `compression` | `String` | `"zstd"` | Алгоритм сжатия по умолчанию (если `compression_enabled = true` и клиент не передал свой): `"zstd"`, `"lz4"`, `"gzip"` или `"none"` |
| `compression_level` | `i32` | `1` | Уровень сжатия ZSTD по умолчанию на стороне прокси (`DoGet` / `DoExchange`): **`1..9`** основной диапазон (`1` — максимальная скорость по умолчанию), полный диапазон `-7..22` |

```toml
[server]
listen_addr = "0.0.0.0:8815"
metrics_addr = "0.0.0.0:9090"
max_decoding_message_size_mb = 64
tcp_nodelay = true
http2_stream_window_size_mb = 16
http2_connection_window_size_mb = 32
compression_enabled = true
compression = "zstd"
compression_level = 1
```

---

### 3.2. `[raft]` — Кластерный консенсус и TTL статуса джобов

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `true` | Включает встроенный Raft-консенсус для синхронизации метаданных и прогресса Spark-джобов |
| `node_id` | `u64` | `1` | Уникальный номер ноды в кластере прокси (`1`, `2`, `3`, ...) |
| `listen_addr` | `String` | `"0.0.0.0:8816"` | Отдельный TCP-адрес и порт для внутреннего трафика Raft |
| `election_timeout_min_ms` | `u64` | `150` | Минимальный таймаут перевыборов лидера (мс) |
| `election_timeout_max_ms` | `u64` | `300` | Максимальный таймаут перевыборов лидера (мс) |
| `heartbeat_interval_ms` | `u64` | `50` | Интервал отправки heartbeat-сообщений от Лидера (мс) |
| `job_stats_ttl_secs` | `u64` | `3600` | Время жизни (TTL в секундах) статистики завершённых джобов в памяти Raft до автоматического удаления |
| `peers` | `Array<RaftPeer>` | `[]` | Список всех равноправных нод кластера (`node_id`, `raft_addr`, `flight_addr`) |

```toml
[raft]
enabled = true
node_id = 1
listen_addr = "0.0.0.0:8816"
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50
job_stats_ttl_secs = 3600

[[raft.peers]]
node_id = 1
raft_addr = "10.0.0.1:8816"
flight_addr = "10.0.0.1:8815"

[[raft.peers]]
node_id = 2
raft_addr = "10.0.0.2:8816"
flight_addr = "10.0.0.2:8815"

[[raft.peers]]
node_id = 3
raft_addr = "10.0.0.3:8816"
flight_addr = "10.0.0.3:8815"
```

---

### 3.3. `[tls]` — Шифрование TLS и взаимный TLS (mTLS)

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `false` | Включает TLS на порту Arrow Flight (`:8815`) |
| `cert_path` | `Option<String>` | `None` | Путь к серверному сертификату X.509 в формате PEM (обязателен при `enabled = true`) |
| `key_path` | `Option<String>` | `None` | Путь к приватному ключу сервера в формате PEM (обязателен при `enabled = true`) |
| `client_ca_path` | `Option<String>` | `None` | Опциональный путь к корневому сертификату CA клиентов; при указании включает обязательную проверку клиентских сертификатов (**mTLS**) |

```toml
[tls]
enabled = true
cert_path = "/etc/as_proxy/certs/server.crt"
key_path = "/etc/as_proxy/certs/server.key"
# Раскомментируйте для включения взаимной аутентификации mTLS:
# client_ca_path = "/etc/as_proxy/certs/client-ca.crt"
```

---

### 3.4. `[auth]` — Аутентификация Handshake и Bearer-токены

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `enabled` | `bool` | `false` | Включает обязательную аутентификацию `Handshake` и проверку `Bearer`-токена на каждом запросе |
| `username` | `String` | `"admin"` | Логин для `Handshake` (`BasicAuth`) |
| `password` | `String` | `"admin"` | Пароль для `Handshake` (проверяется за константное время через `subtle::ConstantTimeEq`) |
| `token_secret` | `String` | `"change-me-hmac-secret"` | Секретный ключ HMAC-SHA256 для подписи Bearer-токенов (должен быть одинаковым на всех нодах прокси) |
| `token_ttl_secs` | `u64` | `3600` | Срок действия выданного Bearer-токена (в секундах) |

```toml
[auth]
enabled = true
username = "spark_writer"
password = "super_strong_password"
token_secret = "shared_cluster_hmac_sha256_secret_key"
token_ttl_secs = 86400
```

---

### 3.5. `[aerospike]` — Пул соединений к кластеру Aerospike

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `hosts` | `String` | `"127.0.0.1:3000"` | Список seed-узлов Aerospike через запятую (`"host1:3000,host2:3000"`) |
| `user` | `Option<String>` | `None` | Опциональное имя пользователя Aerospike |
| `password` | `Option<String>` | `None` | Опциональный пароль пользователя Aerospike |
| `connect_timeout_ms` | `u64` | `5000` | Таймаут установки соединения с узлами Aerospike (мс) |
| `timeout_ms` | `u64` | `1000` | Таймаут сокета на выполнение одной операции `BatchWrite` / `BatchRead` / `Put` / `Scan` (мс) |
| `min_conns_per_node` | `usize` | `16` | Минимальное число прогретых соединений на каждую ноду Aerospike |
| `max_conns_per_node` | `usize` | `512` | Максимальное число открытых соединений на каждую ноду Aerospike |
| `conn_pools_per_node` | `usize` | `4` | Количество независимых пулов соединений на ноду для устранения конкуренции за локи |
| `max_bin_name_len` | `usize` | `64` | Максимальная длина имени бина на стороне прокси (`1..64`; Spark-коннектор по умолчанию проверяет лимит Aerospike в 15 байт) |

```toml
[aerospike]
hosts = "10.10.0.11:3000,10.10.0.12:3000,10.10.0.13:3000"
connect_timeout_ms = 5000
timeout_ms = 2000
min_conns_per_node = 32
max_conns_per_node = 512
conn_pools_per_node = 4
max_bin_name_len = 15
```

---

### 3.6. `[writer]` — MPP-батчинг и параллелизм

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `use_batch_write` | `bool` | `true` | Использовать нативный пакетный протокол `Client::batch` (`BatchWrite`) вместо одиночных `Put` |
| `batch_write_size` | `usize` | `512` | Количество записей в одном MPP под-батче `BatchWrite` / `BatchRead` |
| `max_in_flight_records` | `usize` | `16384` | Глобальный лимит одновременно летящих в Aerospike записей на данной ноде прокси (семафор Backpressure) |
| `max_concurrent_subbatches_per_stream` | `usize` | `16` | Максимальное число параллельных под-батчей внутри одного стрима `DoPut` |

```toml
[writer]
use_batch_write = true
batch_write_size = 512
max_in_flight_records = 16384
max_concurrent_subbatches_per_stream = 16
```

---

### 3.7. `[retry]` — Политика ретраев и экспоненциальный Backoff

| Параметр | Тип | По умолчанию | Описание |
| :--- | :---: | :---: | :--- |
| `max_other_retries` | `u32` | `5` | Максимальное число повторов для **не-перегрузочных** ошибок (ошибки перегрузки `DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `MAX_RETRIES_EXCEEDED` ретраятся **бесконечно**) |
| `initial_backoff_ms` | `u64` | `10` | Начальная задержка перед первым повтором (мс) |
| `max_backoff_ms` | `u64` | `2000` | Максимальная задержка между повторами (мс) |
| `backoff_multiplier` | `f64` | `2.0` | Множитель экспоненциального роста задержки (применяется с полным случайным джиттером) |

```toml
[retry]
max_other_retries = 5
initial_backoff_ms = 10
max_backoff_ms = 2000
backoff_multiplier = 2.0
```

---

## 4. Пример запуска промышленного кластера из 3 нод

Для развёртывания отказоустойчивого кластера из 3 прокси-нод (`10.0.0.1`, `10.0.0.2`, `10.0.0.3`) используйте единый файл `proxy.toml` со списком всех 3 `[[raft.peers]]`, переопределяя только `AS_PROXY_NODE_ID`:

```bash
# Нода 1 (10.0.0.1)
AS_PROXY_NODE_ID=1 ./as_proxy_server --config /etc/as_proxy/proxy.toml

# Нода 2 (10.0.0.2)
AS_PROXY_NODE_ID=2 ./as_proxy_server --config /etc/as_proxy/proxy.toml

# Нода 3 (10.0.0.3)
AS_PROXY_NODE_ID=3 ./as_proxy_server --config /etc/as_proxy/proxy.toml
```

---

## 5. Рекомендации по тюнингу производительности и оптимальные сочетания параметров (`proxy.toml` + `spark_connector`)

Производительность сквозного конвейера **Spark → Arrow Flight → `as_proxy_server` → Aerospike** зависит от согласованности параметров на стороне коннектора и прокси-сервера. Начиная с версии `0.4.3`, прокси автоматически масштабирует размер под-батча по числу бинов (`bin_weight = ceil(num_bins / 10)`), коалесцирует обновления статуса Raft в конец стрима (`propose_batch` без блокировок между стримами) и удерживает буферы сокетов Aerospike до `256 КБ` (`buffer_reclaim_threshold = 262144`) без переаллокаций.

### 5.1. Сводная матрица оптимальных параметров по сценариям (Use Cases)

| Сценарий нагрузки | Параметры Spark-коннектора (`.option(...)`) | Параметры `proxy.toml` (`as_proxy_server`) | Ожидаемый профиль и пояснение |
| :--- | :--- | :--- | :--- |
| **1. Узкие таблицы (1–20 бинов)**<br>Счётчики, профили, KV-признаки (`< 300 байт` на запись) | `connector.batchSize = "8192"`<br>`connector.writerThreads = "2"`..`"4"`<br>`flight.compression = "zstd"`<br>`flight.compression.level = "1"` | `writer.batch_write_size = 512` (или `1024`)<br>`writer.max_in_flight_records = 16384`<br>`writer.max_concurrent_subbatches_per_stream = 16`<br>`aerospike.max_conns_per_node = 512`<br>`aerospike.conn_pools_per_node = 4` | Максимальный RPS (`300K–500K+` строк/с на кластер прокси). Крупные батчи Arrow (`8192`) минимизируют накладные расходы gRPC. |
| **2. Широкие таблицы (50–150 бинов)**<br>Витрины ML-фичей (например, `100` колонок `Float`/`Double`/`Int`, `~1–2 КБ` на запись) | `connector.batchSize = "2048"`..`"4096"`<br>`connector.writerThreads = "2"`<br>`flight.compression = "zstd"`<br>`flight.compression.level = "1"` | `writer.batch_write_size = 512`<br>`writer.max_in_flight_records = 32768`<br>`writer.max_concurrent_subbatches_per_stream = 8`<br>`aerospike.max_conns_per_node = 512`<br>`aerospike.conn_pools_per_node = 8` | При `100` бинах `bin_weight = 10` автоматически нарезает `BatchWrite` по `~51` записи (`~5100` бинов за сетевой вызов), укладываясь в буфер `256 КБ` без фрагментации памяти. |
| **3. Сверхширокие таблицы (150–300+ бинов) или тяжёлые CDT (`Array`/`Map`/`Struct`)** | `connector.batchSize = "1024"`..`"2048"`<br>`connector.writerThreads = "1"`..`"2"`<br>`flight.compression = "zstd"`<br>`flight.compression.level = "1"`..`"3"` | `server.max_decoding_message_size_mb = 128`<br>`writer.batch_write_size = 256`..`512`<br>`writer.max_in_flight_records = 32768`<br>`writer.max_concurrent_subbatches_per_stream = 6`..`8`<br>`aerospike.timeout_ms = 3000` | Уменьшение `batchSize` до `1024..2048` удерживает размер одного Arrow `RecordBatch` в пределах `2–6 МБ` off-heap памяти и исключает пики GC/памяти на экзекьюторах. |
| **4. Большой кластер Spark (`100–200+` ядер) + компактные поды прокси (`1–2 vCPU`, `2 Гб RAM`)** | `connector.batchSize = "2048"`<br>`connector.writerThreads = "1"`..`"2"`<br>`flight.compression = "zstd"` (или `"lz4"`)<br>`flight.compression.level = "1"` | `writer.batch_write_size = 512`<br>`writer.max_in_flight_records = 16384`<br>`writer.max_concurrent_subbatches_per_stream = 4`..`8`<br>`aerospike.max_conns_per_node = 512` | Например, при `40` экзекьюторах × `4` ядра = `160` активных тасков Spark и `writerThreads = 1..2` прокси обслуживает `160..320` потоков без лишнего переключения контекста Tokio на 1 vCPU. |
| **5. Крупные поды прокси (`4–8+ vCPU`, `8+ Гб RAM`) + быстрая локальная сеть (`10–100 GbE`)** | `connector.batchSize = "4096"`<br>`connector.writerThreads = "2"`..`"4"`<br>`flight.compression = "lz4"` (или `"zstd:1"`) | `server.http2_stream_window_size_mb = 32`<br>`server.http2_connection_window_size_mb = 64`<br>`writer.batch_write_size = 512`<br>`writer.max_in_flight_records = 65536`<br>`writer.max_concurrent_subbatches_per_stream = 16`<br>`aerospike.max_conns_per_node = 1024`<br>`aerospike.conn_pools_per_node = 8` | Полная утилизация многоядерных подов прокси и пропускной способности сети при минимальных затратах CPU на сжатие (`lz4`). |
| **6. Параллельное чтение (`DoGet`) и стриминговое обогащение по PK (`DoExchange` Lookup)** | `aerospike.read.numPartitions = "160"`..`"320"` (`1..2x` от числа ядер Spark)<br>`aerospike.read.batchSize = "4096"`<br>Всегда указывайте `.select(...)` нужных бинов! | `server.compression = "zstd"`<br>`server.compression_level = 1`<br>`writer.batch_write_size = 512`<br>`writer.max_in_flight_records = 32768` | Проталкивание проекции колонок (`Column Pruning`) кратно снижает трафик из Aerospike, а `batch_write_size = 512` управляет размером `BatchRead` при `DoExchange`. |

---

### 5.2. Детальные правила расчёта ключевых параметров

1. **Как связать число ядер Spark, `connector.writerThreads` и `writer.max_concurrent_subbatches_per_stream`**:
   * Общее число одновременных стримов `DoPut`, приходящих на один под прокси:
     $$\text{StreamsPerPod} \approx \frac{\text{ActiveSparkTasks} \times \text{connector.writerThreads}}{\text{ProxyPods}}$$
   * Если у вас **160 активных задач Spark** (`40` экзекьюторов по `4` ядра) и **4 пода прокси**, то при `connector.writerThreads = 2` на каждый под прокси приходится `80` одновременных стримов `DoPut`.
   * Если под прокси имеет **1–2 vCPU**, ставьте `connector.writerThreads = "1"` или `"2"` и `writer.max_concurrent_subbatches_per_stream = 4..8`. Завышение `writerThreads` до `4..8` на 160 ядрах создаёт `640..1280` конкурентных стримов, которые конкурируют за кванты CPU планировщика Tokio.
2. **Выбор `connector.batchSize` и `writer.batch_write_size` в зависимости от ширины таблицы**:
   * Прокси автоматически делит каждый входящий `RecordBatch` на под-батчи размера:
     $$\text{chunk\_size} = \max\left(16, \left\lfloor \frac{\text{batch\_write\_size}}{\lceil \text{num\_bins} / 10 \rceil} \right\rfloor\right)$$
   * Для **100 бинов** при `batch_write_size = 512` размер одного `BatchWrite` составляет `51` запись (`~5100` бинов). Для **300 бинов** — `17` записей (`~5100` бинов). Это гарантирует, что сериализованный буфер каждой команды Aerospike не превышает `256 КБ` (`buffer_reclaim_threshold`) и не вызывает лишних аллокаций памяти.
   * На стороне Spark выбирайте `connector.batchSize` так, чтобы произведение `batchSize × num_bins` составляло **`100 000 .. 400 000` ячеек**:
     * `10` бинов → `batchSize = 8192`
     * `100` бинов → `batchSize = 2048..4096`
     * `300` бинов → `batchSize = 1024..2048`
3. **Настройка пула соединений Aerospike (`max_conns_per_node` и `conn_pools_per_node`)**:
   * Для отсутствия ожидания свободных сокетов (`ConnectionPoolEmpty`) держите:
     $$\text{aerospike.max\_conns\_per\_node} \ge 512, \quad \text{aerospike.conn\_pools\_per\_node} = 4..8$$
   * Увеличение `conn_pools_per_node` с `4` до `8` распределяет соединения по 8 независимым мьютексам на каждую ноду Aerospike, полностью устраняя конкуренцию потоков на многоядерных подах.
4. **Выбор алгоритма и уровня компрессии (`flight.compression` и `flight.compression.level`)**:
   * **`zstd` с уровнем `1` (по умолчанию)** — золотой стандарт для большинства инсталляций: сжимает колоночные буферы Arrow в `3–5 раз` со скоростью `> 500 МБ/с` на ядро через нативный JNI (`zstd-jni` off-heap).
   * **`zstd` с уровнем `3..9`** — используйте только при узком сетевом канале между кластером Spark и прокси (межрегиональный трафик), когда на экзекьюторах Spark есть свободный CPU.
   * **`lz4`** — используйте при сверхбыстрой сети (`25–100 GbE`), если CPU экзекьюторов или подов прокси загружен на `> 85%`.
   * **`none` (`compression_enabled = false`)** — отключает сжатие полностью (нулевой расход CPU на кодеки, максимальный сетевой трафик).
