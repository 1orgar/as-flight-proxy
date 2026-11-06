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
  * **Кластер равноправных нод с Raft (`:8816`)**:
    * На отдельном порту (`raft.listen_addr`) работает Raft-консенсус для репликации метаданных кластера (живые ноды, нагрузка, глобальный прогресс и статус каждой выгрузки Spark `job_id` в разрезе всех нод прокси).
    * Любая нода прокси принимает предложения (`propose`), автоматически пересылая их текущему Raft-лидеру, и отдаёт консистентный статус по любому `job_id`.
  * **Нативный `BatchWrite` (`Client::batch`)**: автоматическая группировка ключей по нодам кластера Aerospike 8+ и fallback на одиночный `Client::put`.
  * **Умный по-ключевой ретрай**:
    * Ошибки перегрузки (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `MAX_RETRIES_EXCEEDED`) ретраятся **«до талого» (бесконечно)** с экспоненциальным backoff и полным джиттером, причем из пачки переотправляются только не записавшиеся ключи.
    * Остальные ошибки ретраятся до `max_other_retries` раз, после чего стрим завершается с ошибкой.
  * Режимы записи: `update` (обновление только переданных бинов, при `ignore_nulls = false` по умолчанию `null` отправляет `Value::Nil` и удаляет бин) и `replace` (полная перезапись записи).
  * Автоконвертация `Date` и `Timestamp` в `int64` UTC миллисекунды (`Value::Int(i64)`), вложенных `Array`, `Map`, `Struct` — в Aerospike CDT `List` и `OrderedMap`.
  * Авторизация (`Basic` / `Bearer` HMAC-SHA256 токены через `Handshake`) и TLS 1.2/1.3 (включая mTLS).
  * HTTP-сервер мониторинга (`:9090`) с `/metrics` (Prometheus), `/healthz`, `/cluster/status` и `/jobs/<job_id>`.
* [`spark_connector/`](spark_connector/) — коннектор Spark 3.5 DataSource V2 (`format("aerospike-flight")`):
  * Автоконвертация всех типов Spark `InternalRow` → `Arrow VectorSchemaRoot`.
  * Многопоточная отправка внутри каждого экзекьютор-таска (`connector.writerThreads`) с балансировкой по списку `flight.hosts`.
  * Отслеживание статуса и метрик загрузки через `AerospikeLoadStatus`, Spark SQL UI (`CustomSumMetric`) и Arrow Flight `DoAction`.
  * Сборка в единый fat-JAR (`sbt assembly`) с shading для `io.grpc` и `com.google.protobuf`.

---

## Как получить информацию и статус по загрузке из Spark

Доступны **3 способа** получения статуса и детальной статистики выгрузки:

### Способ 1. Программно на Spark Driver сразу после `.save()` (`AerospikeLoadStatus`)

После завершения `df.write.format("aerospike-flight")...save()` драйвер собирает `WriterCommitMessage` со всех партиций, фиксирует завершение джоба в Raft-кластере прокси и сохраняет итоговый отчёт `LoadJobSummary` в `AerospikeLoadStatus`:

```scala
import com.asproxy.spark.AerospikeLoadStatus

df.repartition(64, col("user_id"))
  .write
  .format("aerospike-flight")
  .option("connector.jobId", "daily-profile-load-2026-11-06")
  .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
  .option("flight.username", "spark_writer")
  .option("flight.password", "change_me_in_prod")
  .option("aerospike.namespace", "test")
  .option("aerospike.set", "users")
  .option("aerospike.keyColumn", "user_id")
  .option("aerospike.writeMode", "update")
  .mode("append")
  .save()

// Получение полной сводки по конкретному jobId (или AerospikeLoadStatus.lastSummary):
val summary = AerospikeLoadStatus.getSummary("daily-profile-load-2026-11-06").get
println(s"Status: ${summary.status}, rows: ${summary.rowsWritten}, overloadRetries: ${summary.overloadRetries}")
println(summary.toJson) // Включает raft_cluster_status с разбивкой строк по каждой ноде прокси!
```

### Способ 2. В реальном времени по RPC (`Arrow Flight DoAction`) или HTTP API любого узла прокси

Во время выполнения выгрузки (или после неё) все равноправные ноды `proxy_server` синхронизируют прогресс через Raft (`:8816`). Опросить статус можно с любой ноды:

1. **Из кода Spark/Scala через Arrow Flight `DoAction`**:
   ```scala
   val jobJson = AerospikeLoadStatus.fetchRemoteJobStatus(options, "daily-profile-load-2026-11-06")
   val clusterJson = AerospikeLoadStatus.fetchClusterStatus(options)
   ```
2. **По HTTP на порту мониторинга (`:9090`) любой ноды прокси**:
   * `GET http://proxy-1:9090/jobs/daily-profile-load-2026-11-06` — статус конкретного джоба (`state`, `total_rows_written`, `total_overload_retries`, `per_node_rows`).
   * `GET http://proxy-1:9090/jobs` — список всех активных и завершённых выгрузок.
   * `GET http://proxy-1:9090/cluster/status` — состояние Raft-кластера (`leader_id`, `current_term`, список нод и их нагрузка).

### Способ 3. В веб-интерфейсе Spark UI (вкладка SQL / DataFrame)

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
