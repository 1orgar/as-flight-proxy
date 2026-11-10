# Руководство по Spark-коннектору (`spark-aerospike-flight-connector`)

[🇬🇧 English version](../en/spark-connector.md) | 🇷🇺 **Русский** | [⬅ Назад к README](../../README.ru.md)

---

## Оглавление

1. [Подключение Fat-JAR в Spark и PySpark](#1-подключение-fat-jar-в-spark-и-pyspark)
2. [Полный справочник опций коннектора](#2-полный-справочник-опций-коннектора)
   - [2.1. Целевой Namespace, Set, PK и лимиты ФЛК](#21-целевой-namespace-set-pk-и-лимиты-флк)
   - [2.2. Подключение Arrow Flight, Auth, TLS и компрессия](#22-подключение-arrow-flight-auth-tls-и-компрессия)
   - [2.3. Параметры записи и DQ-валидации](#23-параметры-записи-и-dq-валидации)
   - [2.4. Параметры чтения (Scan и BatchGet)](#24-параметры-чтения-scan-и-batchget)
   - [2.5. Параметры распределённого обогащения по ключам (`DoExchange` Lookup)](#25-параметры-распределённого-обогащения-по-ключам-doexchange-lookup)
3. [Маппинг типов данных, семантика `NULL` и обработка `NaN`](#3-маппинг-типов-данных-семантика-null-и-обработка-nan)
4. [Правила форматно-логического контроля (ФЛК) на Драйвере](#4-правила-форматно-логического-контроля-флк-на-драйвере)
5. [Примеры кода (PySpark и Scala)](#5-примеры-кода-pyspark-и-scala)
   - [5.1. Пакетная и потоковая запись DataFrame в Aerospike](#51-пакетная-и-потоковая-запись-dataframe-в-aerospike)
   - [5.2. Запуск DQ-валидации (проверки качества записи)](#52-запуск-dq-валидации-проверки-качества-записи)
   - [5.3. Параллельное чтение: скан 4096 партиций, `inferSchema` и PK Pushdown](#53-параллельное-чтение-скан-4096-партиций-inferschema-и-pk-pushdown)
   - [5.4. Распределённое обогащение DataFrame по ключам (`DoExchange` Lookup)](#54-распределённое-обогащение-dataframe-по-ключам-doexchange-lookup)
   - [5.5. Запрос статуса джоба и состояния кластера Raft](#55-запрос-статуса-джоба-и-состояния-кластера-raft)

---

## 1. Подключение Fat-JAR в Spark и PySpark

Используйте собранный Fat-JAR `spark-aerospike-flight-connector-assembly-0.1.0.jar` с **Apache Spark 3.5.x** (Scala 2.12, Java 11/17):

```bash
# Запуск PySpark или spark-submit
pyspark \
  --jars /path/to/spark-aerospike-flight-connector-assembly-0.1.0.jar \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED"
```

Короткое имя формата источника данных: **`"aerospike-flight"`** (или полное имя класса `"com.asproxy.spark.AerospikeFlightDataSource"`).

---

## 2. Полный справочник опций коннектора

### 2.1. Целевой Namespace, Set, PK и лимиты ФЛК

| Опция | Алиасы | Обязательна | По умолчанию | Описание |
| :--- | :--- | :---: | :---: | :--- |
| `aerospike.namespace` | — | **Да** | — | Целевой Namespace в Aerospike (`1..31` ASCII-символ) |
| `aerospike.set` | — | Нет | `""` | Имя Set в Aerospike (`0..63` ASCII-символа) |
| `aerospike.keyColumn` | `aerospike.key_column` | **Да** | — | Имя колонки первичного ключа в DataFrame |
| `aerospike.maxBinNameLen` | `aerospike.max_bin_name_len` | Нет | `15` | Максимальная длина имени бина в байтах UTF-8 (`1..15`) |
| `aerospike.maxBinsPerRecord` | `aerospike.max_bins_per_record` | Нет | `32767` | Максимальное число бинов в одной записи (`1..32767`) |

---

### 2.2. Подключение Arrow Flight, Auth, TLS и компрессия

| Опция | Алиасы | По умолчанию | Описание |
| :--- | :--- | :---: | :--- |
| `flight.hosts` | `aerospike.flight.hosts`, `aerospike.flight.endpoint` | `"127.0.0.1:8815"` | Список `host:port` прокси-нод через запятую (`"proxy1:8815,proxy2:8815,proxy3:8815"`) |
| `flight.username` | `aerospike.flight.username` | `""` | Логин для Arrow Flight `Handshake` |
| `flight.password` | `aerospike.flight.password` | `""` | Пароль для Arrow Flight `Handshake` |
| `flight.timeoutMs` | `aerospike.flight.timeout_ms` | `"60000"` | Таймаут RPC-вызовов Arrow Flight (мс) |
| `flight.compression` | `aerospike.flight.compression`, `compression` | `"zstd"` | Алгоритм сжатия стримов Arrow Flight: `"zstd"` (по умолчанию, off-heap сжатие буферов IPC), `"lz4"` (`LZ4_FRAME`), `"gzip"` (транспортное сжатие gRPC) или `"none"` (`"off"`, `"false"`, `"disabled"`) |
| `flight.compression.enabled` | `aerospike.flight.compression.enabled`, `compression.enabled` | `"true"` | Мастер-переключатель компрессии на клиенте (`"false"` полностью отключает сжатие) |
| `flight.tls.enabled` | `aerospike.flight.tls.enabled` | `"false"` | Включить шифрование TLS (`grpc+tls://`) при подключении к прокси |
| `flight.tls.caCertPath` | `aerospike.flight.tls.ca_cert_path` | Нет | Путь к PEM-файлу корневого сертификата CA на узлах Spark |
| `flight.tls.clientCertPath` | `aerospike.flight.tls.client_cert_path` | Нет | Путь к клиентскому сертификату PEM для взаимного TLS (mTLS) |
| `flight.tls.clientKeyPath` | `aerospike.flight.tls.client_key_path` | Нет | Путь к приватному ключу клиента (PKCS#8 PEM) для взаимного TLS (mTLS) |
| `flight.tls.verifyServer` | `aerospike.flight.tls.verify_server` | `"true"` | Проверять ли цепочку и хост серверного сертификата при TLS |

---

### 2.3. Параметры записи и DQ-валидации

| Опция | Алиасы | По умолчанию | Описание |
| :--- | :--- | :---: | :--- |
| `connector.jobId` | `aerospike.jobId`, `aerospike.job_id`, `job.id` | Авто `as-load-<UUID>` | Уникальный ID джоба для агрегации прогресса и результата DQ в Raft |
| `aerospike.writeMode` | `aerospike.write_mode` | `"update"` | Режим записи: `"update"`, `"replace"`, `"update_only"`, `"replace_only"`, `"create_only"`, `"validate"` (`"dq"`, `"verify"`) |
| `aerospike.dq` | `aerospike.validateOnly`, `aerospike.validate_only`, `connector.dq` | `"false"` | При `"true"` включает режим проверки качества данных (чтение `BatchRead` и побиновая сверка без модификации БД) |
| `aerospike.ignoreNulls` | `aerospike.ignore_nulls` | `"false"` | При `"false"` (по умолчанию) `NULL` в режиме `update` отправляет `Value::Nil` для удаления бина. При `"true"` `NULL`-значения пропускаются |
| `aerospike.ttl` | `aerospike.ttl_seconds` | Дефолт Namespace | TTL записи в секундах (`0` = дефолт namespace, `-1` = Never expire, `-2` = Do not update TTL) |
| `aerospike.sendKey` | `aerospike.send_key` | `"true"` | Сохранять ли исходное значение пользовательского ключа на сервере Aerospike |
| `aerospike.useBatchWrite` | `aerospike.use_batch_write` | Дефолт прокси | Принудительно включить (`"true"`) или выключить (`"false"`) `BatchWrite` для джоба |
| `connector.batchSize` | `aerospike.batchSize`, `aerospike.batch_size` | `"8192"` | Количество строк в одном Arrow `RecordBatch` при записи |
| `connector.writerThreads` | `aerospike.writerThreads`, `aerospike.writer_threads` | `"4"` | Количество параллельных потоков отправки `DoPut` внутри каждой задачи Spark |
| `connector.queueCapacity` | `aerospike.queueCapacity`, `aerospike.queue_capacity` | `max(writerThreads * 2, 4)` | Размер очереди готовых `RecordBatch` между конвертером и потоками отправки |

---

### 2.4. Параметры чтения (Scan и BatchGet)

| Опция | Алиасы | По умолчанию | Описание |
| :--- | :--- | :---: | :--- |
| `aerospike.read.numPartitions` | `aerospike.read.num_partitions` | `"32"` | Число Spark-партиций, на которое делятся 4096 партиций Aerospike (или список ключей `BatchKeys`) (`1..4096`) |
| `aerospike.read.batchSize` | `aerospike.read.batch_size` | `"4096"` | Число строк в одном `RecordBatch`, формируемом прокси при `DoGet` и `DoExchange` |
| `aerospike.read.sampleSize` | `aerospike.read.sample_size` | `"1000"` | Число записей для автоматического вывода схемы (`GetSchema`), если `.schema(...)` не задана |
| `aerospike.read.typeMismatchMode` | `aerospike.read.type_mismatch_mode` | `"fail"` | Поведение при несовпадении типа бина в Aerospike со схемой Spark: `"fail"` (`"strict"`, `"error"`) или `"null"` (`"permissive"`, `"ignore"`) |
| `aerospike.read.keys` | — | Нет | Список первичных ключей (через запятую `"k1,k2"` или JSON-массив `'["k1","k2"]'`) для прямого `BatchGet` чтения |

---

### 2.5. Параметры распределённого обогащения по ключам (`DoExchange` Lookup)

| Опция | Алиасы | По умолчанию | Описание |
| :--- | :--- | :---: | :--- |
| `aerospike.lookup.joinType` | `aerospike.lookup.join_type` | `"left"` | Тип обогащения: `"left"` (`"left_outer"`, сохраняет все строки входного DataFrame, подставляя `null` для отсутствующих ключей) или `"inner"` (возвращает только найденные в Aerospike ключи) |
| `aerospike.lookup.sourceView` | `aerospike.lookup.source_view` | Нет | Имя временного View с ключами при вызове `DoExchange` через `spark.read.format("aerospike-flight")` |
| `aerospike.lookup.binsSchema` | `aerospike.lookup.bins_schema` | Нет | DDL (`"age INT, score DOUBLE"`) или JSON-схема запрашиваемых бинов при использовании `aerospike.lookup.sourceView` |

---

## 3. Маппинг типов данных, семантика `NULL` и обработка `NaN`

| Тип Spark SQL | Тип Apache Arrow | Тип Aerospike (`Value`) | Примечание |
| :--- | :--- | :--- | :--- |
| `BooleanType` | `Boolean` | `Value::Bool(bool)` | При чтении также поддерживает коэрсию из `Int(0/1)` |
| `ByteType`, `ShortType`, `IntegerType`, `LongType` | `Int8`, `Int16`, `Int32`, `Int64` | `Value::Int(i64)` | При записи расширяется до `i64`; при чтении проверяется переполнение |
| `FloatType`, `DoubleType` | `Float32`, `Float64` | `Value::Float(FloatValue)` | IEEE-754 64-bit float; **в режиме DQ `NaN == NaN` считается `true`** |
| `DecimalType(p, s)` | `Decimal128(p, s)` | `Value::String(String)` | Строковое представление с точным сохранением масштаба (например, `"12345.67"`) |
| `StringType` | `Utf8` | `Value::String(String)` | UTF-8 строка |
| `BinaryType` | `Binary` | `Value::Blob(Vec<u8>)` | Массив байт |
| `DateType` | `Date32` (дни) | `Value::Int(i64)` | Хранится как **UTC миллисекунды** (`days * 86_400_000`) |
| `TimestampType` | `Timestamp(Microsecond, "UTC")` | `Value::Int(i64)` | Хранится как **UTC миллисекунды** (`micros / 1_000`) |
| `ArrayType(elemType)` | `List(elemField)` | `Value::List(Vec<Value>)` | Рекурсивный список Aerospike CDT List |
| `MapType(kType, vType)` | `Map(entries)` | `Value::OrderedMap(IndexMap)` | Рекурсивный словарь Aerospike CDT Map |
| `StructType(fields)` | `Struct(fields)` | `Value::OrderedMap(IndexMap)` | Вложенный документ (`field_name -> Value`) |
| `NullType` (`VOID`) | `Null` | `Value::Nil` | Поддерживается для удаления бинов в режиме `update` при `ignoreNulls = false` |

### Семантика `NULL` в колонках бинов
* **Режим `update` / `update_only` при `ignoreNulls = false` (по умолчанию)**: `NULL` в колонке отправляет `Bin(name, Value::Nil)`, что **удаляет этот бин** у записи в Aerospike.
* **Режим `replace` / `replace_only` / `create_only`**: `NULL`-значения **не пересылаются** по сети, так как при замене записи в Aerospike все неназванные бины удаляются автоматически. Строки, где все бины равны `NULL`, пропускаются ещё на стороне Spark.
* **При `ignoreNulls = true`**: `NULL`-бины пропускаются во всех режимах записи.

---

## 4. Правила форматно-логического контроля (ФЛК) на Драйвере

Перед запуском джоба `AerospikeSchemaValidator` проверяет схему на Драйвере и собирает все найденные ошибки в единый отчёт:
1. **Первичный ключ (`aerospike.keyColumn`)**: обязан присутствовать в схеме и иметь тип `StringType`, `ByteType`, `ShortType`, `IntegerType`, `LongType` или `BinaryType`. Значения `NULL` в ключе отвергаются.
2. **Количество бинов**: от `1` до `aerospike.maxBinsPerRecord` (`32767`).
3. **Имена бинов**:
   * Длина от `1` до `15` байт UTF-8 (`aerospike.maxBinNameLen`).
   * Только печатные ASCII-символы (`0x21..0x7E`), без пробелов, управляющих символов и разделителей `:` / `;`.
   * Отсутствие дублирующихся имён колонок.

---

## 5. Примеры кода (PySpark и Scala)

### 5.1. Пакетная и потоковая запись DataFrame в Aerospike

```python
(
    df.write.format("aerospike-flight")
    .option("connector.jobId", "users-load-001")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("flight.username", "spark_writer")
    .option("flight.password", "change_me_in_prod")
    .option("flight.compression", "zstd")           # "zstd" (по умолчанию), "lz4", "gzip" или "none"
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.writeMode", "replace")       # "update", "replace", "create_only" и др.
    .option("aerospike.ttl", "86400")
    .option("connector.batchSize", "8192")
    .option("connector.writerThreads", "4")
    .mode("append")
    .save()
)
```

---

### 5.2. Запуск DQ-валидации (проверки качества записи)

```python
(
    df.write.format("aerospike-flight")
    .option("connector.jobId", "users-dq-001")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.dq", "true")                 # Сверка через BatchRead (NaN == NaN -> True)
    .mode("append")
    .save()
)

# Получение сводки на Драйвере напрямую из PySpark:
summary_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.latestSummaryJson()
print(summary_json)
```

---

### 5.3. Параллельное чтение: скан 4096 партиций, `inferSchema` и PK Pushdown

```python
# 1. Полный параллельный скан 4096 партиций с авто-выводом схемы и Column Pruning
df_scan = (
    spark.read.format("aerospike-flight")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .option("aerospike.read.numPartitions", "64")
    .option("aerospike.read.sampleSize", "1000")
    .load()
    .select("user_id", "age", "score")              # Из Aerospike читаются только бины "age" и "score"!
)

# 2. Прямое чтение по ключам (BatchGet) через Filter Pushdown (=, <=>, IN, AND, OR)
df_keys = (
    spark.read.format("aerospike-flight")
    .schema("user_id STRING, age INT, score DOUBLE, tags ARRAY<STRING>")
    .option("flight.hosts", "proxy-1:8815,proxy-2:8815,proxy-3:8815")
    .option("aerospike.namespace", "prod")
    .option("aerospike.set", "users")
    .option("aerospike.keyColumn", "user_id")
    .load()
    .filter("user_id IN ('u_100', 'u_200', 'u_300')")
)
```

---

### 5.4. Распределённое обогащение DataFrame по ключам (`DoExchange` Lookup)

Обогащение существующего `DataFrame` с первичными ключами (`keys_df`) бинами из Aerospike напрямую с экзекьюторов по двунаправленному стриму `DoExchange` (без шаффла и без сбора ключей на Драйвер):

**PySpark (через `AerospikeFlightLookup.lookup`):**
```python
from pyspark.sql import DataFrame

keys_df = spark.createDataFrame(
    [(101, "u_1"), (102, "u_2"), (103, "u_missing")],
    ["request_id", "user_id"],
)

lookup_options = {
    "flight.hosts": "proxy-1:8815,proxy-2:8815,proxy-3:8815",
    "flight.compression": "zstd",
    "aerospike.namespace": "prod",
    "aerospike.set": "users",
    "aerospike.keyColumn": "user_id",
    "aerospike.lookup.joinType": "left",            # "left" или "inner"
    "aerospike.read.batchSize": "4096",
}

jdf = spark._jvm.com.asproxy.spark.AerospikeFlightLookup.lookup(
    keys_df._jdf,
    "age INT, score DOUBLE, tags ARRAY<STRING>",
    lookup_options,
)
enriched_df = DataFrame(jdf, spark)
enriched_df.show()
```

**Scala:**
```scala
import com.asproxy.spark.AerospikeFlightLookup

val enrichedDf = AerospikeFlightLookup.lookup(
  keysDf = keysDf,
  binsSchemaDdlOrJson = "age INT, score DOUBLE, tags ARRAY<STRING>",
  options = Map(
    "flight.hosts" -> "proxy-1:8815,proxy-2:8815,proxy-3:8815",
    "aerospike.namespace" -> "prod",
    "aerospike.set" -> "users",
    "aerospike.keyColumn" -> "user_id",
    "aerospike.lookup.joinType" -> "left"
  )
)
```

---

### 5.5. Запрос статуса джоба и состояния кластера Raft

```python
# Запрос реплицированного статуса джоба из Raft:
job_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.fetchRemoteJobStatusJson(
    "proxy-1:8815", "users-dq-001", "spark_writer", "change_me_in_prod", False
)
print(job_json)

# Запрос статуса и лидера Raft-кластера прокси:
cluster_json = spark._jvm.com.asproxy.spark.AerospikeLoadStatus.fetchClusterStatusJson(
    "proxy-1:8815", "spark_writer", "change_me_in_prod", False
)
print(cluster_json)
```
