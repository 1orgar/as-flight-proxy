# Наблюдаемость: Логирование и метрики Prometheus

[🇬🇧 English version](../en/observability.md) | 🇷🇺 **Русский** | [⬅ Назад к README](../../README.ru.md)

---

## Оглавление

1. [Структурированное логирование на Драйвере и Экзекьюторах Spark](#1-структурированное-логирование-на-драйвере-и-экзекьюторах-spark)
2. [Кастомные метрики в Spark SQL UI](#2-кастомные-метрики-в-spark-sql-ui)
3. [Метрики Prometheus прокси-сервера (`:9090/metrics`)](#3-метрики-prometheus-прокси-сервера-9090metrics)
4. [Проверка работоспособности (`:9090/healthz`)](#4-проверка-работоспособности-9090healthz)
5. [Примеры PromQL-запросов и правил алертинга](#5-примеры-promql-запросов-и-правил-алертинга)

---

## 1. Структурированное логирование на Драйвере и Экзекьюторах Spark

`spark_connector` пишет информативные логи через стандартный `org.apache.spark.internal.Logging` (Log4j2) с чёткими префиксами ролей и операций, чтобы в логах YARN / Kubernetes было сразу видно, что происходит на Драйвере, а что — на Экзекьюторах:

| Префикс лога | Где пишется | Что логируется |
| :--- | :---: | :--- |
| `[Driver][ФЛК]` | Spark Driver | Результат проверки схемы (`namespace.set`, `keyColumn`, `binsCount`, `writeMode`, `omitNullsInFlight`) или полный перечень ошибок ФЛК |
| `[Driver][Write]` | Spark Driver | Старт и завершение пакетной/потоковой записи, скорость (`rows/sec`), `rowsWritten`, `batchesSent`, `overloadRetries`, `otherRetries`, а также итог DQ-сверки (`dqPassed`, количество расхождений) |
| `[Executor][Write]` | Spark Executor | Инициализация таска (`partition`, `task`, `compression`, `writerThreads`, назначенные `endpoints`), предупреждения при ретраях на прокси и коммит партиции (`elapsedMs`, `rowsWritten`, `batchesSent`) |
| `[Driver][InferSchema]` | Spark Driver | Время вывода схемы (`elapsedMs`), адрес прокси, `sampleSize` и полученная DDL-схема |
| `[Driver][ScanPlan]` | Spark Driver | Применённый Column Pruning (`requiredColumns`), пушдаун фильтров по PK (`pushed` и `postScanFilters`), план партиционирования (`4096-partition scan`, `BatchKeys` или `DoExchange Lookup`) |
| `[Executor][Read]` | Spark Executor | Открытие и закрытие стрима `DoGet` для партиции, режим (`PartitionRange` или `BatchKeys`), `compression`, `rowsRead`, `batchesRead`, `elapsedMs` |
| `[Driver][Lookup]` | Spark Driver | Планирование распределённого `DoExchange` обогащения (`keyColumn`, `joinType`, `inputColumns`, `lookupBins`, `outputSchema`) |
| `[Executor][Lookup]` | Spark Executor | Открытие и закрытие стрима `DoExchange` на экзекьюторе, `compression`, `inputRowsSent`, `inputBatchesSent`, `outputRowsReceived`, `outputBatchesReceived`, `elapsedMs` |

---

## 2. Кастомные метрики в Spark SQL UI

Коннектор публикует собственные метрики прогресса, которые отображаются прямо в графе выполнения **Spark SQL UI**:

### Метрики записи и DQ (`AerospikeFlightBatchWrite` / `StreamingWrite`)
* `as_rows_written` — число записанных в Aerospike строк
* `as_batches_sent` — число отправленных на прокси Arrow `RecordBatch`
* `as_overload_retries` — число ретраев перегрузки (`DEVICE_OVERLOAD` / `KEY_BUSY`)
* `as_other_retries` — число прочих ретраев
* `as_rows_validated` — число проверенных в режиме DQ строк
* `as_matched_rows` — число совпавших в режиме DQ строк
* `as_mismatched_rows` — число несовпавших в режиме DQ строк

### Метрики чтения (`AerospikeFlightScan`)
* `as_rows_read` — число прочитанных через `DoGet` строк
* `as_batches_read` — число полученных от прокси Arrow `RecordBatch`

---

## 3. Метрики Prometheus прокси-сервера (`:9090/metrics`)

Каждая нода `as_proxy_server` отдаёт **21 метрику Prometheus** по адресу `GET http://<proxy-host>:9090/metrics`:

### Метрики записи (`DoPut`)
| Имя метрики | Тип | Описание |
| :--- | :---: | :--- |
| `as_proxy_records_written_total` | `counter` | Общее количество успешно записанных в Aerospike записей |
| `as_proxy_batches_processed_total` | `counter` | Общее количество обработанных на запись Arrow `RecordBatch` |
| `as_proxy_write_duration_seconds_sum` | `counter` | Суммарное время выполнения под-батчей записи в Aerospike (в секундах) |
| `as_proxy_write_duration_seconds_count` | `counter` | Общее количество выполненных под-батчей записи в Aerospike |

### Метрики чтения и Lookup (`DoGet` / `DoExchange` / `GetSchema`)
| Имя метрики | Тип | Описание |
| :--- | :---: | :--- |
| `as_proxy_records_read_total` | `counter` | Общее количество прочитанных из Aerospike записей (`DoGet` + `DoExchange`) |
| `as_proxy_read_batches_total` | `counter` | Общее количество сформированных и отправленных клиенту выходных `RecordBatch` |
| `as_proxy_lookup_keys_requested_total` | `counter` | Общее количество ключей, запрошенных через `DoExchange` Lookup |
| `as_proxy_lookup_keys_found_total` | `counter` | Общее количество ключей, найденных в Aerospike при `DoExchange` Lookup |
| `as_proxy_read_duration_seconds_sum` | `counter` | Суммарное время выполнения операций чтения/скана/лукапа (в секундах) |
| `as_proxy_read_duration_seconds_count` | `counter` | Общее количество выполненных операций чтения/скана/лукапа |

### Метрики DQ-валидации (`validate_only = true`)
| Имя метрики | Тип | Описание |
| :--- | :---: | :--- |
| `as_proxy_records_validated_total` | `counter` | Общее количество проверенных строк в режиме DQ |
| `as_proxy_validation_mismatches_total` | `counter` | Общее количество обнаруженных расхождений (`MissingKey` + `BinMismatch`) |

### Метрики ретраев, ошибок и активной нагрузки
| Имя метрики | Тип | Описание |
| :--- | :---: | :--- |
| `as_proxy_overload_retries_total` | `counter` | Количество ретраев перегрузки (`DEVICE_OVERLOAD`, `KEY_BUSY`, `NO_MORE_CONNECTIONS`, `MAX_RETRIES_EXCEEDED`) |
| `as_proxy_other_retries_total` | `counter` | Количество ретраев прочих ошибок |
| `as_proxy_failed_records_total` | `counter` | Количество записей, завершившихся фатальной ошибкой после исчерпания `max_other_retries` |
| `as_proxy_in_flight_records` | `gauge` | Текущее количество записей, находящихся в полёте к Aerospike |
| `as_proxy_active_streams` | `gauge` | Текущее количество активных стримов Arrow Flight (`DoPut`, `DoGet`, `DoExchange`) |

### Метрики связности с Aerospike и состояния Raft
| Имя метрики | Тип | Описание |
| :--- | :---: | :--- |
| `as_proxy_aerospike_connected` | `gauge` | `1` — есть подключение к кластеру Aerospike, `0` — нет связи |
| `as_proxy_raft_node_id` | `gauge` | Сконфигурированный `node_id` данной ноды прокси |
| `as_proxy_raft_term` | `gauge` | Текущий терм (term) Raft |
| `as_proxy_raft_is_leader` | `gauge` | `1`, если данная нода является Raft-лидером, иначе `0` |
| `as_proxy_raft_commit_index` | `gauge` | Текущий индекс закоммиченного лога Raft |

---

## 4. Проверка работоспособности (`:9090/healthz`)

* `GET http://<proxy-host>:9090/healthz`
  * Возвращает **`200 OK`** (`{"status":"ok"}`), когда клиент прокси подключён к кластеру Aerospike.
  * Возвращает **`503 Service Unavailable`** (`{"status":"unhealthy"}`), если связь со всеми узлами Aerospike потеряна.

---

## 5. Примеры PromQL-запросов и правил алертинга

```promql
# 1. Суммарная скорость записи по кластеру прокси (записей/сек)
sum(rate(as_proxy_records_written_total[1m]))

# 2. Суммарная скорость чтения и Lookup (записей/сек)
sum(rate(as_proxy_records_read_total[1m]))

# 3. Средняя задержка записи под-батча в Aerospike (мс)
1000 * sum(rate(as_proxy_write_duration_seconds_sum[1m])) / sum(rate(as_proxy_write_duration_seconds_count[1m]))

# 4. Доля найденных ключей при Lookup (Hit Ratio, %)
100 * sum(rate(as_proxy_lookup_keys_found_total[5m])) / sum(rate(as_proxy_lookup_keys_requested_total[5m]))

# 5. Контроль наличия ровно одного Raft-лидера в кластере прокси
sum(as_proxy_raft_is_leader)
```
