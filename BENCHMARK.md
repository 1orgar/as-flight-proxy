# Результаты E2E-бенчмарка (`as-flight-proxy` v0.1.0)

В этом документе зафиксированы результаты матричного End-to-End (E2E) нагрузочного тестирования полного тракта передачи и валидации данных:
**Apache Spark 3.5.2 (PySpark / Catalyst) → Apache Arrow Flight (`DoPut` gRPC) → Кластер `as_proxy_server` (Rust + Raft) → Кластер Aerospike 8.0 CE**.

Исходный скрипт бенчмарка: [`e2e/run_e2e_bench.py`](e2e/run_e2e_bench.py)  
Сырые JSON-результаты прогонов: [`e2e/benchmark_results.json`](e2e/benchmark_results.json)  
Конфигурация стенда: [`docker-compose.e2e.yml`](docker-compose.e2e.yml)

---

## 1. Конфигурация тестового стенда

* **Аппаратная платформа**: Apple Silicon (`aarch64` / `arm64`, 10 CPU cores), Docker Engine (Linux 7.0.14-linuxkit).
* **Кластер Aerospike 8.0.0.20 Community Edition** ([`e2e/aerospike.conf`](e2e/aerospike.conf)):
  * **2 ноды** (`aerospike-1`, `aerospike-2`), объединенные в `mesh`-кластер (4096 партиций).
  * Namespace `test`: `replication-factor = 2`, `storage-engine memory` (`data-size = 2G`).
* **Кластер `as_proxy_server` (Rust)** ([`e2e/proxy-1.toml`](e2e/proxy-1.toml), [`e2e/proxy-2.toml`](e2e/proxy-2.toml)):
  * **2 равноправные ноды** (`proxy-1`, `proxy-2`), объединенные в Raft-кластер на портах `:8816`.
  * Авторизация Arrow Flight включена (`Basic` Handshake → подписанный HMAC-SHA256 `Bearer` токен).
  * Настройки записи: `batch_write_size = 128`, `max_batch_concurrency = 32`, `max_in_flight_records = 16384`.
* **Apache Spark 3.5.2** (`scala 2.12.18`, `OpenJDK 17.0.12`):
  * Режим `local[8]`, память драйвера `2 GB`.
  * Все тестовые датасеты предварительно материализованы в памяти JVM (`.cache()` + `.count()`) перед запуском таймера, чтобы замерять чистую скорость коннектора, сети Arrow Flight, прокси и Aerospike.

### Схемы тестовых данных
1. **Flat (6 колонок)**:
   * `pk: String`, `age: Int`, `balance: Double`, `active: Boolean`, `name: String`, `ts: Timestamp (UTC ms)`.
2. **Nested CDT + `NaN` (6 колонок со сложной вложенностью)**:
   * `pk: String`, `age: Int`, `balance: Double` (каждая 20-я строка — `NaN`), `tags: Array<String>`, `scores: Map<String, Double>`, `profile: Struct<tier: String, pts: Long>`.

---

## 2. Сводная таблица результатов (15 конфигураций)

| ID | Сценарий / Что тестируем | Строк | Схема | Прокси нод | Партиций × Потоков (`P × T`) | Arrow `batchSize` | `useBatchWrite` | Режим | Бачей Flight | Распределение по нодам (`node1 / node2`) | Время (сек) | Throughput (**строк/сек**) |
| :--- | :--- | ---: | :--- | :---: | :---: | ---: | :---: | :--- | ---: | :--- | ---: | ---: |
| **B01** | Одиночные `Put` (fallback) | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 4 096 | `false` | `update` | 28 | `49 152 / 50 848` | `0.826 s` | **121 045 /s** |
| **B02** | Нативный `BatchWrite` | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 4 096 | `true` | `update` | 28 | `46 328 / 53 672` | `0.251 s` | **397 931 /s** |
| **B03** | 1 нода прокси | 100 000 | Flat (6 колонок) | 1 | `4 × 2` (8 стримов) | 8 192 | `true` | `update` | 16 | `100 000 / 0` | `0.248 s` | **403 672 /s** |
| **B04** | 2 ноды прокси (Raft MPP) | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 8 192 | `true` | `update` | 16 | `50 000 / 50 000` | `0.282 s` | **354 943 /s** |
| **B05** | Малый Arrow-батч (`512`) | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 512 | `true` | `update` | 196 | `48 376 / 51 624` | `0.321 s` | **311 518 /s** |
| **B06** | Средний Arrow-батч (`2048`) | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 2 048 | `true` | `update` | 52 | `45 904 / 54 096` | `0.245 s` | **408 600 /s** |
| **B07** | Крупный Arrow-батч (`16384`) | 100 000 | Flat (6 колонок) | 2 | `4 × 2` (8 стримов) | 16 384 | `true` | `update` | 8 | `65 536 / 34 464` | `0.272 s` | **367 721 /s** |
| **B08** | Низкий параллелизм (`2 × 1`) | 100 000 | Flat (6 колонок) | 2 | `2 × 1` (2 стрима) | 8 192 | `true` | `update` | 14 | `50 000 / 50 000` | `0.273 s` | **366 952 /s** |
| **B09** | Высокий параллелизм (`8 × 2`) | 100 000 | Flat (6 колонок) | 2 | `8 × 2` (16 стримов) | 8 192 | `true` | `update` | 16 | `46 116 / 53 884` | `0.257 s` | **388 533 /s** |
| **B10** | Сверхвысокий параллелизм (`8 × 4`) | 100 000 | Flat (6 колонок) | 2 | `8 × 4` (32 стрима) | 8 192 | `true` | `update` | 16 | `37 924 / 62 076` | `0.347 s` | **288 521 /s** |
| **B11** | Режим полной перезаписи (`replace`) | 100 000 | Flat (6 колонок) | 2 | `8 × 2` (16 стримов) | 8 192 | `true` | `replace` | 16 | `42 232 / 57 768` | `0.249 s` | **400 924 /s** |
| **B12** | **DQ Валидация** (чтение + сверка) | 100 000 | Flat (6 колонок) | 2 | `8 × 2` (16 стримов) | 8 192 | `true` | `DQ (Validate)` | 16 | `50 000 / 50 000` | `0.128 s` | **780 924 /s** |
| **B13** | Сложные CDT (`List` + `Map` + `Struct`) | 100 000 | Nested CDT + `NaN` | 2 | `4 × 2` (8 стримов) | 8 192 | `true` | `update` | 16 | `57 768 / 42 232` | `0.749 s` | **133 556 /s** |
| **B14** | **DQ Валидация сложных CDT + `NaN`** | 100 000 | Nested CDT + `NaN` | 2 | `4 × 2` (8 стримов) | 8 192 | `true` | `DQ (Validate)` | 16 | `50 000 / 50 000` | `0.406 s` | **246 363 /s** |
| **B15** | Длительная нагрузка (`250 000` строк) | 250 000 | Flat (6 колонок) | 2 | `8 × 4` (32 стрима) | 8 192 | `true` | `replace` | 32 | `123 482 / 126 518` | `0.894 s` | **279 675 /s** |

> Во всех 15 прогонах: `overload_retries = 0`, `other_retries = 0`, расхождений при DQ-проверках — `0` (`dq_passed = true`).

---

## 3. Анализ результатов и рекомендации по тюнингу

1. **Нативный `BatchWrite` против одиночных `Put` (`B02` vs `B01` — ускорение в `3.29x`)**:
   * Переключение `aerospike.useBatchWrite = true` повышает скорость записи со **121 045 строк/сек** до **397 931 строк/сек** за счет группировки ключей в мульти-записи Aerospike.
2. **Выбор размера Arrow `RecordBatch` (`connector.batchSize`, `B05`–`B07`)**:
   * Малый батч (`512`, **311 518 строк/сек**) увеличивает частоту синхронных подтверждений `PutResult` и Raft-репликаций статистики (`196` батчей на 100k строк).
   * Оптимальный диапазон `connector.batchSize` — **`2 048` – `8 192` строк** (**388 533 – 408 600 строк/сек**).
3. **Параллелизм потоков записи (`connector.writerThreads`, `B08`–`B10`)**:
   * Оптимальное значение для большинства задач — **`connector.writerThreads = 2`** на каждую партицию Spark.
   * При избыточном числе потоков (`writerThreads = 4` при `8` партициях на небольшом датасете в `100k` строк) создается 32 параллельных gRPC-соединения с `Handshake`, хотя самих батчей всего 16, что дает лишний оверхед на установку соединений.
4. **Производительность Data Quality валидации (`B12` и `B14`)**:
   * В режиме `aerospike.dq = true` прокси выполняет параллельный `Client::batch_get` и сравнивает значения бинов в памяти (с учетом `NaN == NaN` на всех уровнях вложенности).
   * На плоской схеме скорость DQ-проверки достигает **780 924 строк/сек** (`100 000` строк за `0.128 сек`), на сложных вложенных CDT (`List` + `Map` + `Struct`) — **246 363 строк/сек**.

---

## 4. Как воспроизвести бенчмарк локально

```bash
# 1. Поднять 2 ноды Aerospike 8.0 CE и 2 ноды as_proxy_server
docker compose -f docker-compose.e2e.yml up -d aerospike-1 aerospike-2 proxy-1 proxy-2

# 2. Запустить матричный бенчмарк в контейнере Spark 3.5
docker compose -f docker-compose.e2e.yml run --rm --entrypoint /bin/bash spark-e2e -c "
  set -e
  pip3 install --quiet pyarrow
  /opt/spark/bin/spark-submit \
    --master 'local[8]' \
    --driver-memory 2g \
    --driver-java-options '--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
    --conf 'spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED' \
    --jars /e2e/artifacts/spark-aerospike-flight-connector-assembly-0.1.0.jar \
    /e2e/run_e2e_bench.py
"

# 3. Остановить стенд
docker compose -f docker-compose.e2e.yml down
```
