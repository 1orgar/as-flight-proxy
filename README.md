# as-flight-proxy: Spark → Arrow Flight → Rust MPP Proxy → Aerospike 8+ CE

[![CI & Build Artifacts](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/Rust-1.88%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala)](https://www.scala-lang.org/)
[![Arrow Flight](https://img.shields.io/badge/Apache%20Arrow%20Flight-gRPC%20%2F%20TLS-00A8E8?logo=apache)](https://arrow.apache.org/)
[![Aerospike](https://img.shields.io/badge/Aerospike%20CE-8.0%2B-C2185B)](https://aerospike.com/)

Высокопроизводительная система пакетной и потоковой выгрузки данных из **Apache Spark (3.5 / Scala 2.12)** в **Aerospike 8+ Community Edition** через промежуточный **MPP-прокси на Rust** по протоколу **Apache Arrow Flight (gRPC / HTTP2 + TLS)**.

---

## Состав репозитория

* [`proxy_server/`](proxy_server/) — MPP-сервер на Rust (`tokio`, `tonic`, `arrow-flight 60`, `aerospike 3.0 async`):
  * Нативный `BatchWrite` (`Client::batch`) с автоматической группировкой ключей по нодам кластера Aerospike 8+ и fallback на одиночный `Client::put`.
  * **Умный по-ключевой ретрай**:
    * Ошибки перегрузки (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_AVAILABLE_CONNECTIONS_TO_NODE`, `MAX_RETRIES_EXCEEDED`) ретраятся **«до талого» (бесконечно)** с экспоненциальным backoff и полным джиттером, причем из пачки переотправляются только не записавшиеся ключи.
    * Остальные ошибки ретраятся до `max_other_retries` раз, после чего стрим завершается с ошибкой.
  * Режимы записи: `update` (обновление только переданных бинов, при `ignore_nulls = false` по умолчанию `null` отправляет `Value::Nil` и удаляет бин) и `replace` (полная перезапись записи).
  * Автоконвертация `Date` и `Timestamp` в `int64` UTC миллисекунды (`Value::Int(i64)`), вложенных `Array`, `Map`, `Struct` — в Aerospike CDT `List` и `OrderedMap`.
  * Авторизация (`Basic` / `Bearer` HMAC-SHA256 токены через `Handshake`) и TLS 1.2/1.3 (включая mTLS).
  * HTTP-сервер мониторинга (`:9090`) с `/metrics` (Prometheus) и `/healthz`.
* [`spark_connector/`](spark_connector/) — коннектор Spark 3.5 DataSource V2 (`format("aerospike-flight")`):
  * Автоконвертация всех типов Spark `InternalRow` → `Arrow VectorSchemaRoot`.
  * Многопоточная отправка внутри каждого экзекьютор-таска (`connector.writerThreads`) с балансировкой по списку `flight.hosts`.
  * Сборка в единый fat-JAR (`sbt assembly`) с shading для `io.grpc` и `com.google.protobuf`.

---

## Артефакты CI (GitHub Actions)

При каждом пуше и PR workflow [CI & Build Artifacts](https://github.com/1orgar/as-flight-proxy/actions/workflows/ci.yml) автоматически прогоняет тесты (Rust + Scala) и собирает готовые к деплою артефакты:
1. `as_proxy_server-linux-x86_64` — релизный бинарник прокси-сервера для архитектуры **x86_64** (`x86_64-unknown-linux-gnu`).
2. `as_proxy_server-linux-arm64` — релизный бинарник прокси-сервера для архитектуры **ARM64** (`aarch64-unknown-linux-gnu`).
3. `spark-aerospike-flight-connector-jar` — собранный с shading Fat-JAR коннектора (`spark-aerospike-flight-connector-assembly-0.1.0.jar`).

---

## Быстрый старт

### 1. Сборка и запуск `proxy_server` (Rust)

```bash
cd proxy_server
cargo test
cargo build --release

# Запуск с конфигурационным файлом
./target/release/as_proxy_server --config config.example.toml
```

### 2. Сборка `spark_connector` (Scala 2.12 / Spark 3.5)

```bash
cd spark_connector
sbt test
sbt assembly
# Готовый fat-JAR появится в target/scala-2.12/spark-aerospike-flight-connector-assembly-0.1.0.jar
```

### 3. Пример записи DataFrame из Spark

```scala
df.repartition(64, col("user_id"))
  .write
  .format("aerospike-flight")
  .option("flight.hosts", "proxy-1:8815,proxy-2:8815")
  .option("flight.username", "spark_writer")
  .option("flight.password", "change_me_in_prod")
  .option("flight.tls.enabled", "true")
  .option("flight.tls.caCertPath", "/etc/certs/ca.crt")
  .option("aerospike.namespace", "test")
  .option("aerospike.set", "users")
  .option("aerospike.keyColumn", "user_id")
  .option("aerospike.writeMode", "update")     // "update" (только бины) или "replace" (строка целиком)
  .option("aerospike.ignoreNulls", "false")    // false (по умолчанию): null в update удаляет бин (Value::Nil)
  .option("aerospike.ttl", "86400")            // секунды TTL (-1 = NeverExpire, -2 = DontUpdate, 0 = default)
  .option("aerospike.sendKey", "true")
  .option("connector.batchSize", "8192")
  .option("connector.writerThreads", "4")
  .mode("append")
  .save()
```
