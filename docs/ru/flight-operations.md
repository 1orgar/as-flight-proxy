# Работа с Arrow Flight напрямую из Python (`pyarrow.flight`)

> 🌐 **Язык / Language**: **[🇷🇺 Русский](../ru/flight-operations.md)** | **[🇬🇧 English](../en/flight-operations.md)**

Прокси-сервер `as_proxy_server` реализует стандартный gRPC-протокол **Apache Arrow Flight**. Это означает, что помимо Spark-коннектора вы можете напрямую обращаться к любой ноде прокси из обычных Python-скриптов, DAG-ов **Apache Airflow**, **Prefect**, **Dagster**, **Jupyter Notebook** или микросервисов с помощью стандартной библиотеки `pyarrow.flight`.

Ниже приведено полное руководство с готовыми примерами кода для всех операций Arrow Flight:
1. [Подключение, авторизация (`Handshake`), TLS/mTLS и заголовки сжатия](#1-подключение-авторизация-handshake-tlsmtls-и-заголовки-сжатия)
2. [Получение списка поддерживаемых команд (`list_actions`)](#2-получение-списка-поддерживаемых-команд-list_actions)
3. [Проверка состояния кластера Raft и здоровья нод (`DoAction: get_cluster_status`)](#3-проверка-состояния-кластера-raft-и-здоровья-нод-doaction-get_cluster_status)
4. [Проверка статуса джобов, результатов DQ и сброс меты (`DoAction: get_job_status`, `init_job`)](#4-проверка-статуса-джобов-результатов-dq-и-сброс-меты-doaction-get_job_status-init_job)
5. [Автоматическое определение схемы сета Aerospike (`GetSchema`)](#5-автоматическое-определение-схемы-сета-aerospike-getschema)
6. [Чтение данных из Aerospike в `pyarrow.Table` / `pandas.DataFrame` (`DoGet`)](#6-чтение-данных-из-aerospike-в-pyarrowtable--pandasdataframe-doget)
7. [Стриминговое обогащение по первичным ключам (`DoExchange` Lookup)](#7-стриминговое-обогащение-по-первичным-ключам-doexchange-lookup)
8. [Прямая пакетная запись и DQ-валидация из Python (`DoPut`)](#8-прямая-пакетная-запись-и-dq-валидация-из-python-doput)

---

## 1. Подключение, авторизация (`Handshake`), TLS/mTLS и заголовки сжатия

Установка пакета:
```bash
pip install pyarrow pandas
```

Базовый клиент-хелпер, который подключается к прокси по `grpc://` или `grpc+tls://`, выполняет `Handshake` (`authenticate_basic_token` $\rightarrow$ получает подписанный `Bearer` токен) и формирует `FlightCallOptions`:

```python
import base64
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as fl


def connect_flight_proxy(
    host_port: str,
    username: str = "",
    password: str = "",
    tls_enabled: bool = False,
    ca_cert_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    verify_server: bool = True,
    compression: str = "zstd",   # "zstd", "lz4", "gzip" или "none"
    timeout_sec: float = 30.0,
) -> tuple[fl.FlightClient, fl.FlightCallOptions]:
    scheme = "grpc+tls" if tls_enabled else "grpc"
    location = f"{scheme}://{host_port}"

    client_kwargs = {}
    if tls_enabled:
        if ca_cert_path:
            client_kwargs["tls_root_certs"] = Path(ca_cert_path).read_bytes()
        if client_cert_path and client_key_path:
            client_kwargs["cert_chain"] = Path(client_cert_path).read_bytes()
            client_kwargs["private_key"] = Path(client_key_path).read_bytes()
        if not verify_server:
            client_kwargs["disable_server_verification"] = True

    client = fl.FlightClient(location, **client_kwargs)

    headers: list[tuple[bytes, bytes]] = []
    if username:
        # Handshake возвращает кортеж (b'authorization', b'Bearer <token>')
        bearer_header = client.authenticate_basic_token(username, password)
        headers.append(bearer_header)

    if compression:
        headers.append((b"x-as-compression", compression.encode("utf-8")))

    call_options = fl.FlightCallOptions(headers=headers, timeout=timeout_sec)
    return client, call_options
```

---

## 2. Получение списка поддерживаемых команд (`list_actions`)

Вызов `client.list_actions()` возвращает список всех RPC-команд управления состоянием Raft, поддерживаемых прокси-сервером:

```python
client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    for action_info in client.list_actions(options=opts):
        print(f"{action_info.type:20s} -> {action_info.description}")
finally:
    client.close()
```

Вывод:
```text
init_job             -> Initialize or reset Raft-replicated metadata for a Spark job_id
get_job_status       -> Get Raft-replicated load status for a Spark job_id
get_cluster_status   -> Get Raft cluster topology, node metrics, and all jobs
complete_job         -> Mark a Spark load job_id as completed in Raft
fail_job             -> Mark a Spark load job_id as failed in Raft
```

---

## 3. Проверка состояния кластера Raft и здоровья нод (`DoAction: get_cluster_status`)

Команда `get_cluster_status` возвращает полный слепок состояния Raft-узла: кто сейчас является лидером (`leader_id`), текущий терм (`current_term`), индекс коммита (`commit_index`), статус всех прокси-нод и словарь всех активных/завершённых джобов.

```python
def get_raft_cluster_status(client: fl.FlightClient, opts: fl.FlightCallOptions) -> dict:
    action = fl.Action("get_cluster_status", b"")
    results = list(client.do_action(action, options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    cluster = get_raft_cluster_status(client, opts)
    print("ID текущей ноды:", cluster["node_id"])
    print("Роль в Raft:", cluster["role"])                 # "Leader" | "Follower" | "Candidate"
    print("ID лидера Raft:", cluster["leader_id"])
    print("Текущий Term:", cluster["current_term"])
    print("Commit Index:", cluster["commit_index"])
    print("Активных джобов в памяти Raft:", len(cluster["state"]["jobs"]))
finally:
    client.close()
```

Пример JSON-ответа `get_cluster_status`:
```json
{
  "node_id": 1,
  "role": "Leader",
  "current_term": 3,
  "leader_id": 1,
  "commit_index": 42,
  "last_applied": 42,
  "state": {
    "nodes": {
      "1": {
        "node_id": 1,
        "flight_addr": "as-flight-proxy-0.as-flight-proxy:3030",
        "raft_addr": "as-flight-proxy-0.as-flight-proxy:8816",
        "aerospike_connected": true,
        "in_flight_records": 0,
        "records_written_total": 10000000,
        "updated_at_ms": 1762796800000
      }
    },
    "jobs": {
      "users-load-001": {
        "job_id": "users-load-001",
        "namespace": "dev",
        "set_name": "users",
        "is_dq_validation": false,
        "state": "completed",
        "total_rows_written": 10000000,
        "total_batches_processed": 9766,
        "total_overload_retries": 0,
        "total_other_retries": 0,
        "per_node_rows": { "1": 2500000, "2": 2500000, "3": 2500000, "4": 2500000 }
      }
    }
  }
}
```

---

## 4. Проверка статуса джобов, результатов DQ и сброс меты (`DoAction: get_job_status`, `init_job`)

### 4.1. Запрос статуса конкретного `job_id` (`get_job_status`)
Статус любого джоба реплицируется по Raft и доступен на **любой** ноде прокси в течение `raft.job_stats_ttl_secs`:

```python
def get_job_status(client: fl.FlightClient, opts: fl.FlightCallOptions, job_id: str) -> dict:
    action = fl.Action("get_job_status", job_id.encode("utf-8"))
    results = list(client.do_action(action, options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    status = get_job_status(client, opts, "users-load-001")
    print("Состояние:", status["state"])                     # "running" | "completed" | "failed"
    print("Записано строк:", status["total_rows_written"])
    print("Обработано батчей:", status["total_batches_processed"])
    print("Ретраев перегрузки:", status["total_overload_retries"])
    print("Распределение по нодам прокси:", status["per_node_rows"])

    # Если это был DQ-джоб (aerospike.dq = true):
    if status["is_dq_validation"]:
        print("DQ Пройден:", status["dq_passed"])
        print("Проверено строк:", status["total_rows_validated"])
        print("Отсутствующих ключей:", status["total_missing_keys"])
        print("Расхождений бинов:", status["total_bin_mismatch_rows"])
        for sample in status.get("dq_mismatch_samples", []):
            print(f"  Несовпадение на ключе {sample['key']}: {sample['reason']}")
finally:
    client.close()
```

### 4.2. Инициализация / перезатирание меты джоба в Raft (`init_job` / `reset_job`)
При старте новой Spark-джобы с тем же `connector.jobId` коннектор автоматически вызывает `init_job` на драйвере, полностью обнуляя старую мету этого `jobId` в Raft. Вы также можете вызвать `init_job` напрямую из Python:

```python
def reset_job_in_raft(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    job_id: str,
    namespace: str,
    set_name: str,
    is_dq_validation: bool = False,
) -> dict:
    payload = json.dumps({
        "job_id": job_id,
        "namespace": namespace,
        "set_name": set_name,
        "is_dq_validation": is_dq_validation,
    }).encode("utf-8")
    results = list(client.do_action(fl.Action("init_job", payload), options=opts))
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))
```

### 4.3. Завершение или пометка джоба упавшим (`complete_job` / `fail_job`)
```python
# Пометить джоб успешно завершённым:
client.do_action(fl.Action("complete_job", b"users-load-001"), options=opts)

# Пометить джоб упавшим с текстом ошибки:
fail_payload = json.dumps({
    "job_id": "users-load-001",
    "namespace": "dev",
    "set_name": "users",
    "error": "Timeout waiting for upstream source"
}).encode("utf-8")
list(client.do_action(fl.Action("fail_job", fail_payload), options=opts))
```

---

## 5. Автоматическое определение схемы сета Aerospike (`GetSchema`)

Метод `client.get_schema(descriptor)` сэмплирует `sample_size` записей из Aerospike и возвращает готовую схему `pyarrow.Schema`:

```python
def infer_aerospike_schema(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str = "id",
    sample_size: int = 1000,
) -> pa.Schema:
    cmd_bytes = json.dumps({
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "sample_size": sample_size,
    }).encode("utf-8")
    descriptor = fl.FlightDescriptor.for_command(cmd_bytes)
    schema_result = client.get_schema(descriptor, options=opts)
    return schema_result.schema


client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    arrow_schema = infer_aerospike_schema(client, opts, namespace="dev", set_name="users", key_column="id", sample_size=500)
    print(arrow_schema)
finally:
    client.close()
```

---

## 6. Чтение данных из Aerospike в `pyarrow.Table` / `pandas.DataFrame` (`DoGet`)

Для чтения через `DoGet` необходимо сериализовать целевую `pyarrow.Schema` в формат Arrow IPC Base64 и передать JSON-тикет `ReadTicket` в `client.do_get(ticket)`.

Вспомогательная функция сериализации `pyarrow.Schema` в `schema_ipc_base64`:
```python
def encode_schema_ipc_base64(schema: pa.Schema) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        pass
    return base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")
```

### 6.1. Быстрое чтение по списку первичных ключей (`BatchKeys`)
Читает конкретные первичные ключи и только нужные бины за миллисекунды через нативный `BatchRead`:

```python
def read_keys_to_arrow(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str,
    keys: list[str | int | bytes],
    target_schema: pa.Schema,
    batch_size: int = 1024,
    compression: str = "zstd",
) -> pa.Table:
    formatted_keys = []
    for k in keys:
        if isinstance(k, int):
            formatted_keys.append({"type": "int", "value": k})
        elif isinstance(k, bytes):
            formatted_keys.append({"type": "bytes", "value": base64.b64encode(k).decode("ascii")})
        else:
            formatted_keys.append({"type": "string", "value": str(k)})

    bins = [f.name for f in target_schema if f.name != key_column]
    ticket_payload = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "bins": bins,
        "read_mode": {
            "mode": "batch_keys",
            "keys": formatted_keys,
        },
        "batch_size": batch_size,
        "type_mismatch_mode": "null",   # "fail" или "null"
        "schema_ipc_base64": encode_schema_ipc_base64(target_schema),
        "compression": compression,
    }
    ticket = fl.Ticket(json.dumps(ticket_payload).encode("utf-8"))
    reader = client.do_get(ticket, options=opts)
    return reader.read_all()


# Пример использования:
target_schema = pa.schema([
    pa.field("id", pa.utf8(), nullable=False),
    pa.field("name", pa.utf8(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
])

client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    table = read_keys_to_arrow(
        client,
        opts,
        namespace="dev",
        set_name="users",
        key_column="id",
        keys=["user_1", "user_2", "user_3"],
        target_schema=target_schema,
    )
    df_pandas = table.to_pandas()
    print(df_pandas)
finally:
    client.close()
```

### 6.2. Сканирование диапазона партиций Aerospike (`PartitionRange`)
В Aerospike 4096 партиций (`0..4095`). Вы можете вычитать как весь сет (`begin_partition=0, partition_count=4096`), так и отдельный срез партиций в несколько потоков:

```python
def scan_partitions_to_arrow(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    namespace: str,
    set_name: str,
    key_column: str,
    target_schema: pa.Schema,
    begin_partition: int = 0,
    partition_count: int = 4096,
    batch_size: int = 4096,
    compression: str = "zstd",
) -> pa.Table:
    bins = [f.name for f in target_schema if f.name != key_column]
    ticket_payload = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "bins": bins,
        "read_mode": {
            "mode": "partition_range",
            "begin_partition": begin_partition,
            "partition_count": partition_count,
        },
        "batch_size": batch_size,
        "type_mismatch_mode": "null",
        "schema_ipc_base64": encode_schema_ipc_base64(target_schema),
        "compression": compression,
    }
    ticket = fl.Ticket(json.dumps(ticket_payload).encode("utf-8"))
    reader = client.do_get(ticket, options=opts)
    return reader.read_all()
```

---

## 7. Стриминговое обогащение по первичным ключам (`DoExchange` Lookup)

Метод `client.do_exchange` открывает двунаправленный стрим: вы отправляете `RecordBatch`-и с первичными ключами (и любыми сквозными колонками вашего датасета), а прокси параллельно дочитывает запрошенные бины из Aerospike и стримит обратно обогащённые `RecordBatch`-и:

```python
def enrich_table_via_do_exchange(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    keys_table: pa.Table,
    namespace: str,
    set_name: str,
    key_column: str,
    bins_schema: pa.Schema,
    join_type: str = "left",        # "left" или "inner"
    batch_size: int = 4096,
    compression: str = "zstd",
) -> pa.Table:
    lookup_params = {
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "join_type": join_type,
        "batch_size": batch_size,
        "type_mismatch_mode": "null",
        "bins_schema_ipc_base64": encode_schema_ipc_base64(bins_schema),
        "compression": compression,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(lookup_params).encode("utf-8"))

    writer, reader = client.do_exchange(descriptor, options=opts)
    with writer:
        writer.begin(keys_table.schema)
        for batch in keys_table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)
        writer.done_writing()
        return reader.read_all()


# Пример использования:
input_table = pa.table({
    "event_id": [101, 102, 103],
    "id": ["user_1", "user_missing", "user_3"],
})
bins_to_fetch = pa.schema([
    pa.field("name", pa.utf8(), nullable=True),
    pa.field("score", pa.float64(), nullable=True),
])

client, opts = connect_flight_proxy("as-flight-proxy-0.as-flight-proxy:3030", "user", "pass", tls_enabled=True, verify_server=False)
try:
    enriched_table = enrich_table_via_do_exchange(
        client,
        opts,
        keys_table=input_table,
        namespace="dev",
        set_name="users",
        key_column="id",
        bins_schema=bins_to_fetch,
        join_type="left",
    )
    print(enriched_table.to_pandas())
finally:
    client.close()
```

---

## 8. Прямая пакетная запись и DQ-валидация из Python (`DoPut`)

Через `client.do_put` можно напрямую записывать `pyarrow.Table` / `pandas.DataFrame` в Aerospike или запускать DQ-сверку (`validate_only=True`), получая JSON-подтверждение (`PutBatchAck`) на каждый отправленный батч:

```python
def write_or_validate_table_via_do_put(
    client: fl.FlightClient,
    opts: fl.FlightCallOptions,
    table: pa.Table,
    job_id: str,
    namespace: str,
    set_name: str,
    key_column: str,
    write_mode: str = "update",     # "update", "replace", "update_only", "replace_only", "create_only"
    ttl_seconds: int | None = 86400,
    ignore_nulls: bool = False,
    validate_only: bool = False,    # True = режим DQ-валидации без модификации данных в Aerospike
    batch_size: int = 2048,
) -> list[dict]:
    # 1. Инициализируем/перезатираем мету job_id в Raft перед стартом:
    reset_job_in_raft(client, opts, job_id, namespace, set_name, is_dq_validation=validate_only)

    write_params = {
        "job_id": job_id,
        "namespace": namespace,
        "set_name": set_name,
        "key_column": key_column,
        "write_mode": write_mode,
        "ttl_seconds": ttl_seconds,
        "send_key": True,
        "ignore_nulls": ignore_nulls,
        "use_batch_write": True,
        "validate_only": validate_only,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(write_params).encode("utf-8"))

    acks: list[dict] = []
    writer, reader = client.do_put(descriptor, table.schema, options=opts)
    with writer:
        for batch in table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)
            ack_buf = reader.read()
            if ack_buf is not None:
                acks.append(json.loads(ack_buf.to_pybytes().decode("utf-8")))
        writer.done_writing()

    # 2. Фиксируем завершение джоба в Raft:
    list(client.do_action(fl.Action("complete_job", job_id.encode("utf-8")), options=opts))
    return acks
```
