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
  * Режимы записи: `update` (обновление только переданных бинов, при `ignore_nulls = false` по умолчанию `null` отправляет `Value::Nil` и удаляет бин) и `replace` (полная перезапись записи).
  * Автоконвертация `Date` и `Timestamp` в `int64` UTC миллисекунды (`Value::Int(i64)`), вложенных `Array`, `Map`, `Struct` — в Aerospike CDT `List` и `OrderedMap`.
  * Авторизация (`Basic` / `Bearer` HMAC-SHA256 токены через `Handshake`) и TLS 1.2/1.3 (включая mTLS).
  * HTTP-сервер мониторинга (`:9090`) только с `/metrics` (Prometheus) и `/healthz`.
* [`spark_connector/`](spark_connector/) — коннектор Spark 3.5 DataSource V2 (`format("aerospike-flight")`):
  * Автоконвертация всех типов Spark `InternalRow` → `Arrow VectorSchemaRoot`.
  * Многопоточная отправка внутри каждого экзекьютор-таска (`connector.writerThreads`) с балансировкой по списку `flight.hosts`.
  * Чтение статуса загрузки в PySpark/Scala как нативного Spark DataFrame (`spark.read.format("aerospike-flight")...load()`), через `spark._jvm` (`AerospikeLoadStatus`) и через Spark SQL UI (`CustomSumMetric`).
  * Сборка в единый fat-JAR (`sbt assembly`) с shading для `io.grpc` и `com.google.protobuf`.

---

## Работа из PySpark: выгрузка и получение статистики

### 1. Выгрузка DataFrame из PySpark в Aerospike

```python
job_id = "daily-profile-load-2026-11-06"

(
    df.repartition(64, "user_id")
    .write.format("aerospike-flight")
    .option("connector.jobId", job_id)
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("flight.username", "spark_writer")
    .option("flight.password", "change_me_in_prod")
    .option("aerospike.namespace", "test")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.writeMode", "update")      # "update" или "replace"
    .option("aerospike.ignoreNulls", "false")     # по умолчанию false: null в update удаляет бин
    .option("connector.batchSize", "8192")
    .option("connector.writerThreads", "4")
    .mode("append")
    .save()
)
```

### 2. Получение статуса загрузки в PySpark

Доступны **3 удобных способа** без использования HTTP-порта метрик:

#### Способ А. Нативное чтение статуса как Spark DataFrame (`spark.read.format("aerospike-flight")`)
Коннектор реализует `SupportsRead`, который опрашивает Raft-кластер прокси по Arrow Flight `DoAction` (и локальный реестр драйвера) и возвращает DataFrame со статистикой:

```python
# Статус конкретного job_id (или без connector.jobId — список всех джобов в Raft с учетом TTL):
status_df = (
    spark.read.format("aerospike-flight")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("flight.username", "spark_writer")
    .option("flight.password", "change_me_in_prod")
    .option("connector.jobId", job_id)
    .load()
)

status_df.show(truncate=False)
# Схема: job_id, state, namespace, set_name, rows_written, batches_processed,
#        overload_retries, other_retries, elapsed_ms, raw_json (включая per_node_rows из Raft)
```

#### Способ Б. Получение JSON-сводки в Python-словарь через `spark._jvm`
Сразу после `.save()` или во время работы джоба можно вызвать статические методы `AerospikeLoadStatus` через Py4J:

```python
import json

status_api = spark._jvm.com.asproxy.spark.AerospikeLoadStatus

# 1. Итоговая сводка драйвера по завершённому джобу (включая слепок Raft):
summary = json.loads(status_api.getSummaryJson(job_id))
print(summary["status"], summary["rows_written"], summary["overload_retries"])

# 2. Запрос живого Raft-статуса напрямую с кластера прокси по Arrow Flight DoAction:
live_raft_job = json.loads(
    status_api.fetchRemoteJobStatusJson(
        "proxy-1:8815,proxy-2:8815", job_id, "spark_writer", "change_me_in_prod", False
    )
)
print(live_raft_job["per_node_rows"])
```

#### Способ В. В веб-интерфейсе Spark UI (вкладка SQL / DataFrame)
Коннектор регистрирует нативные метрики Spark DataSource V2 (`CustomSumMetric`), которые отображаются прямо в графе выполнения запроса в **Spark UI**:
* `as_rows_written` — количество записанных строк;
* `as_batches_sent` — количество отправленных Arrow `RecordBatch`;
* `as_overload_retries` — количество ретраев перегрузки (`DEVICE_OVERLOAD` / `KEY_BUSY`);
* `as_other_retries` — количество прочих ретраев.

---

## Артефакты CI (GitHub Actions)

При каждом пуше и PR workflow [CI & Build Artifacts](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml) автоматически прогоняет тесты (Rust + Scala) и собирает готовые к деплою артефакты:
1. `as_proxy_server-linux-x86_64` — релизный бинарник прокси-сервера для архитектуры **x86_64** (`x86_64-unknown-linux-gnu`).
2. `as_proxy_server-linux-arm64` — релизный бинарник прокси-сервера для архитектуры **ARM64** (`aarch64-unknown-linux-gnu`).
3. `spark-aerospike-flight-connector-jar` — собранный с shading Fat-JAR коннектора (`spark-aerospike-flight-connector-assembly-0.1.0.jar`).
