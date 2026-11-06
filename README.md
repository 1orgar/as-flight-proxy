# as-flight-proxy: Spark → Arrow Flight → Rust MPP Proxy → Aerospike 8+ CE

[![CI & Build Artifacts](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/Rust-1.88%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala)](https://www.scala-lang.org/)
[![Arrow Flight](https://img.shields.io/badge/Apache%20Arrow%20Flight-gRPC%20%2F%20TLS-00A8E8?logo=apache)](https://arrow.apache.org/)
[![Aerospike](https://img.shields.io/badge/Aerospike%20CE-8.0%2B-C2185B)](https://aerospike.com/)

Высокопроизводительная система пакетной и потоковой выгрузки данных из **Apache Spark (3.5 / Scala 2.12)** в **Aerospike 8+ Community Edition** через кластер равноправных **MPP-прокси на Rust** по протоколу **Apache Arrow Flight (gRPC / HTTP2 + TLS)** с координацией метаданных по протоколу **Raft**.

---

## Состав репозитория

* [`proxy_server/`](proxy_server/) — MPP-сервер на Rust (`tokio`, `tonic`, `arrow-flight 60`, `aerospike 3.0 async`):
  * **Кластер равноправных нод с Raft (`:8816`) и TTL статистики**:
    * На отдельном порту (`raft.listen_addr`) работает Raft-консенсус для репликации метаданных кластера (живые ноды, нагрузка, глобальный прогресс и статус каждой выгрузки Spark `job_id` в разрезе всех нод прокси).
    * Любая нода прокси принимает предложения (`propose`), автоматически пересылая их текущему Raft-лидеру, и отдаёт консистентный статус по любому `job_id` через Arrow Flight `DoAction`.
    * Поддерживается автоматическая очистка устаревшей статистики по TTL (`raft.job_stats_ttl_secs`, по умолчанию `3600` секунд).
  * **Нативный `BatchWrite` (`Client::batch`)**: автоматическая группировка ключей по нодам кластера Aerospike 8+ и fallback на одиночный `Client::put`.
  * **Умный по-ключевой ретрай**:
    * Ошибки перегрузки (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `MAX_RETRIES_EXCEEDED`) ретраятся **«до талого» (бесконечно)** с экспоненциальным backoff и полным джиттером, причем из пачки переотправляются только не записавшиеся ключи.
    * Остальные ошибки ретраятся до `max_other_retries` раз, после чего стрим завершается с ошибкой.
  * Режимы записи: `update` (обновление только переданных бинов, при `ignore_nulls = false` по умолчанию `null` отправляет `Value::Nil` и удаляет бин), `replace` (полная перезапись записи) и **`validate` / `dq`** (проверка качества данных без модификации базы: чтение батчами из Aerospike и побиновое сравнение со строками из Spark, при котором сравнение двух `NaN` всегда даёт `true`, в том числе внутри вложенных `List`/`Map`/`Struct`).
  * Автоконвертация `Date` и `Timestamp` в `int64` UTC миллисекунды (`Value::Int(i64)`), вложенных `Array`, `Map`, `Struct` — в Aerospike CDT `List` и `OrderedMap`.
  * Авторизация (`Basic` / `Bearer` HMAC-SHA256 токены через `Handshake`) и TLS 1.2/1.3 (включая mTLS).
  * HTTP-сервер мониторинга (`:9090`) только с `/metrics` (Prometheus) и `/healthz`.
* [`spark_connector/`](spark_connector/) — коннектор Spark 3.5 DataSource V2 (`format("aerospike-flight")`):
  * Автоконвертация всех типов Spark `InternalRow` → `Arrow VectorSchemaRoot`.
  * Многопоточная отправка внутри каждого экзекьютор-таска (`connector.writerThreads`) с балансировкой по списку `flight.hosts`.
  * Отслеживание статуса загрузки через `pyarrow.flight` (`DoAction`), `spark._jvm` (`AerospikeLoadStatus`) и Spark SQL UI (`CustomSumMetric`).
  * Сборка в единый fat-JAR (`sbt assembly`) с shading для `io.grpc` и `com.google.protobuf`.

---

## Работа из PySpark: выгрузка и получение статистики

### 1. Выгрузка DataFrame из PySpark в Aerospike (с TLS и авторизацией)

```python
job_id = "daily-profile-load-2026-11-06"

(
    df.repartition(64, "user_id")
    .write.format("aerospike-flight")
    .option("connector.jobId", job_id)
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("flight.username", "spark_writer")
    .option("flight.password", "change_me_in_prod")
    .option("flight.tls.enabled", "true")
    .option("flight.tls.caCertPath", "/etc/certs/ca.crt")
    .option("aerospike.namespace", "test")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.writeMode", "update")      # "update", "replace" или "validate" ("dq")
    .option("aerospike.dq", "false")              # если "true" — режим проверки записи (DQ)
    .option("aerospike.ignoreNulls", "false")     # по умолчанию false: null в update удаляет бин
    .option("connector.batchSize", "8192")
    .option("connector.writerThreads", "4")
    .mode("append")
    .save()
)
```

### 2. Получение статуса загрузки из Python / PySpark через `pyarrow.flight` (с TLS и авторизацией)

Для опроса статуса из Raft-кластера прокси используется стандартный `pyarrow.flight.FlightClient`:

```python
import json
import pyarrow.flight as fl

# 1. Подключение с TLS (или "grpc://proxy-1:8815" без TLS)
with open("/etc/certs/ca.crt", "rb") as f:
    root_certs = f.read()

# Если используется mTLS (взаимная аутентификация), передаются также cert_chain и private_key:
client = fl.FlightClient(
    location="grpc+tls://proxy-1:8815",
    tls_root_certs=root_certs,
    # cert_chain=open("/etc/certs/client.crt", "rb").read(),
    # private_key=open("/etc/certs/client.key", "rb").read(),
    # disable_server_verification=False,  # True для тестовых самоподписанных сертификатов без CA
)

# 2. Авторизация (Handshake -> возвращает кортеж заголовка ('authorization', 'Bearer <token>'))
auth_token_tuple = client.authenticate_basic_token("spark_writer", "change_me_in_prod")
call_options = fl.FlightCallOptions(headers=[auth_token_tuple], timeout=10.0)

# 3. Запрос статуса конкретного job_id из Raft (работает на любой ноде прокси)
results = client.do_action(
    fl.Action("get_job_status", job_id.encode("utf-8")),
    options=call_options,
)
job_status = json.loads(next(results).body.to_pybytes())
print("Статус выгрузки:", job_status)
# {
#   "job_id": "daily-profile-load-2026-11-06",
#   "state": "completed",
#   "is_dq_validation": true,
#   "total_rows_validated": 1500000,
#   "total_matched_rows": 1500000,
#   "total_mismatched_rows": 0,
#   "total_missing_keys": 0,
#   "total_bin_mismatch_rows": 0,
#   "dq_passed": true,
#   "dq_mismatch_samples": [],
#   "per_node_rows": {"1": 500000, "2": 500000, "3": 500000}, ...
# }

# 4. Запрос статуса всего Raft-кластера прокси и всех активных джобов (в рамках TTL)
cluster_results = client.do_action(
    fl.Action("get_cluster_status", b""),
    options=call_options,
)
cluster_status = json.loads(next(cluster_results).body.to_pybytes())
print("Raft leader:", cluster_status["leader_id"], "Ноды:", cluster_status["state"]["nodes"])
```

---

## Артефакты CI (GitHub Actions)

При каждом пуше и PR workflow [CI & Build Artifacts](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml) автоматически прогоняет тесты (Rust + Scala) и собирает готовые к деплою артефакты:
1. `as_proxy_server-linux-x86_64` — релизный бинарник прокси-сервера для архитектуры **x86_64** (`x86_64-unknown-linux-gnu`).
2. `as_proxy_server-linux-arm64` — релизный бинарник прокси-сервера для архитектуры **ARM64** (`aarch64-unknown-linux-gnu`).
3. `spark-aerospike-flight-connector-jar` — собранный с shading Fat-JAR коннектора (`spark-aerospike-flight-connector-assembly-0.1.0.jar`).
