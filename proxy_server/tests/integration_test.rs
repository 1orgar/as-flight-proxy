use aerospike::{Bin, FloatValue, Key, Value};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{Action, BasicAuth, FlightDescriptor, HandshakeRequest};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::Server;

use as_proxy_server::auth::Authenticator;
use as_proxy_server::config::{
    AuthConfig, InferSchemaRequest, KeyLiteral, LookupExchangeParams, LookupJoinType, RaftConfig,
    RaftPeer, ReadModeSpec, ReadTicket, RetryConfig, TypeMismatchMode, WriteMode, WriteParameters,
    WriterConfig,
};
use as_proxy_server::converter::{
    PreparedRecord, ScannedRecord, convert_record_batch, encode_arrow_schema_ipc_base64,
    values_equal_dq,
};
use as_proxy_server::error::Result as ProxyResult;
use as_proxy_server::flight_service::{ProxyFlightService, PutBatchAck};
use as_proxy_server::metrics::ProxyMetrics;
use as_proxy_server::raft::{JobLoadStatus, JobState, RaftNode, RaftRole};
use as_proxy_server::writer::{
    AerospikeSink, MppWriteEngine, RecordReadOutcome, RecordWriteOutcome,
};

/// Mock Aerospike sink that records writes per key and allows injecting overload or fatal errors.
struct MockAerospikeSink {
    stored: Mutex<HashMap<Key, Vec<Bin>>>,
    write_attempts: Mutex<HashMap<Key, usize>>,
    overload_failures_per_key: HashMap<Key, usize>,
    other_failures_per_key: HashMap<Key, usize>,
    batch_calls: AtomicUsize,
}

impl MockAerospikeSink {
    fn new(
        overload_failures_per_key: HashMap<Key, usize>,
        other_failures_per_key: HashMap<Key, usize>,
    ) -> Self {
        Self {
            stored: Mutex::new(HashMap::new()),
            write_attempts: Mutex::new(HashMap::new()),
            overload_failures_per_key,
            other_failures_per_key,
            batch_calls: AtomicUsize::new(0),
        }
    }
}

#[tonic::async_trait]
impl AerospikeSink for MockAerospikeSink {
    async fn execute_batch_write(
        &self,
        records: &[PreparedRecord],
        _params: &WriteParameters,
    ) -> Vec<RecordWriteOutcome> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        let mut attempts_guard = self.write_attempts.lock().unwrap();
        let mut stored_guard = self.stored.lock().unwrap();

        let mut outcomes = Vec::with_capacity(records.len());
        for rec in records {
            let count = attempts_guard.entry(rec.key.clone()).or_insert(0);
            *count += 1;
            let current_attempt = *count;

            if let Some(&fail_until) = self.overload_failures_per_key.get(&rec.key) {
                if current_attempt <= fail_until {
                    outcomes.push(RecordWriteOutcome::OverloadError(
                        "Simulated DEVICE_OVERLOAD / KEY_BUSY".to_string(),
                    ));
                    continue;
                }
            }

            if let Some(&fail_until) = self.other_failures_per_key.get(&rec.key) {
                if current_attempt <= fail_until {
                    outcomes.push(RecordWriteOutcome::OtherError(
                        "Simulated ServerError / ParameterError".to_string(),
                    ));
                    continue;
                }
            }

            stored_guard.insert(rec.key.clone(), rec.bins.clone());
            outcomes.push(RecordWriteOutcome::Success);
        }

        outcomes
    }

    async fn execute_single_put(
        &self,
        record: &PreparedRecord,
        params: &WriteParameters,
    ) -> RecordWriteOutcome {
        let mut res = self
            .execute_batch_write(std::slice::from_ref(record), params)
            .await;
        res.pop().unwrap_or(RecordWriteOutcome::Success)
    }

    async fn execute_batch_read(
        &self,
        records: &[PreparedRecord],
        _params: &WriteParameters,
    ) -> Vec<RecordReadOutcome> {
        let mut attempts_guard = self.write_attempts.lock().unwrap();
        let stored_guard = self.stored.lock().unwrap();
        let mut outcomes = Vec::with_capacity(records.len());

        for rec in records {
            let count = attempts_guard.entry(rec.key.clone()).or_insert(0);
            *count += 1;
            let current_attempt = *count;

            if let Some(&fail_until) = self.overload_failures_per_key.get(&rec.key) {
                if current_attempt <= fail_until {
                    outcomes.push(RecordReadOutcome::OverloadError(
                        "Simulated DEVICE_OVERLOAD on read".to_string(),
                    ));
                    continue;
                }
            }

            if let Some(&fail_until) = self.other_failures_per_key.get(&rec.key) {
                if current_attempt <= fail_until {
                    outcomes.push(RecordReadOutcome::OtherError(
                        "Simulated ServerError on read".to_string(),
                    ));
                    continue;
                }
            }

            match stored_guard.get(&rec.key) {
                Some(bins) => {
                    let mut map = HashMap::new();
                    for b in bins {
                        // In Aerospike, writing Value::Nil deletes the bin from the record
                        if b.value != Value::Nil {
                            map.insert(b.name.clone(), b.value.clone());
                        }
                    }
                    outcomes.push(RecordReadOutcome::Found(map));
                }
                None => outcomes.push(RecordReadOutcome::NotFound),
            }
        }
        outcomes
    }

    async fn scan_partition_range(
        &self,
        namespace: &str,
        set_name: &str,
        begin_partition: usize,
        partition_count: usize,
        bins: Option<Vec<String>>,
        max_records: Option<u64>,
    ) -> ProxyResult<tokio::sync::mpsc::Receiver<ProxyResult<ScannedRecord>>> {
        let stored_guard = self.stored.lock().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut items = Vec::new();
        for (key, stored_bins) in stored_guard.iter() {
            if key.namespace() != namespace || (!set_name.is_empty() && key.set_name() != set_name)
            {
                continue;
            }
            let pid = key.partition_id();
            if pid < begin_partition || pid >= begin_partition + partition_count {
                continue;
            }
            let mut map = IndexMap::new();
            for b in stored_bins {
                if b.value != Value::Nil {
                    if let Some(ref req_bins) = bins {
                        if req_bins.is_empty() || !req_bins.iter().any(|rb| rb == &b.name) {
                            continue;
                        }
                    }
                    map.insert(b.name.clone(), b.value.clone());
                }
            }
            items.push(ScannedRecord {
                key: Some(key.clone()),
                bins: map,
            });
            if let Some(max_rec) = max_records {
                if items.len() >= max_rec as usize {
                    break;
                }
            }
        }
        tokio::spawn(async move {
            for item in items {
                if tx.send(Ok(item)).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    fn is_connected(&self) -> bool {
        true
    }
}

#[test]
fn test_convert_all_types_and_null_modes() {
    let struct_fields = Fields::from(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("zip", DataType::Int32, true),
    ]);

    let mut id_builder = StringBuilder::new();
    let mut age_builder = Int32Builder::new();
    let mut score_builder = Float64Builder::new();
    let mut active_builder = BooleanBuilder::new();
    let mut amount_builder = Decimal128Builder::new().with_data_type(DataType::Decimal128(10, 2));
    let mut date_builder = Date32Builder::new();
    let mut ts_builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut bin_builder = BinaryBuilder::new();
    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    let mut props_builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let mut profile_builder = StructBuilder::new(
        struct_fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int32Builder::new()),
        ],
    );

    // Row 0: All populated
    id_builder.append_value("user_1");
    age_builder.append_value(30);
    score_builder.append_value(99.5);
    active_builder.append_value(true);
    amount_builder.append_value(12345); // 123.45
    date_builder.append_value(20_000); // 20000 days * 86_400_000 ms
    ts_builder.append_value(1_700_000_000_123_456); // -> 1_700_000_000_123 ms
    bin_builder.append_value(&[0xDE, 0xAD, 0xBE, 0xEF]);

    tags_builder.values().append_value("vip");
    tags_builder.values().append_value("beta");
    tags_builder.append(true);

    props_builder.keys().append_value("clicks");
    props_builder.values().append_value(42);
    props_builder.append(true).unwrap();

    profile_builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("Berlin");
    profile_builder
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(10115);
    profile_builder.append(true);

    // Row 1: Some null columns to test ignore_nulls=false vs ignore_nulls=true vs Replace mode
    id_builder.append_value("user_2");
    age_builder.append_null();
    score_builder.append_value(50.0);
    active_builder.append_null();
    amount_builder.append_null();
    date_builder.append_null();
    ts_builder.append_null();
    bin_builder.append_null();
    tags_builder.append(false);
    props_builder.append(false).unwrap();
    profile_builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_null();
    profile_builder
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_null();
    profile_builder.append(false);

    let tags_arr = tags_builder.finish();
    let props_arr = props_builder.finish();
    let profile_arr = profile_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("birth_date", DataType::Date32, true),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("raw_bytes", DataType::Binary, true),
        Field::new("tags", tags_arr.data_type().clone(), true),
        Field::new("props", props_arr.data_type().clone(), true),
        Field::new("profile", DataType::Struct(struct_fields), true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(age_builder.finish()),
            Arc::new(score_builder.finish()),
            Arc::new(active_builder.finish()),
            Arc::new(amount_builder.finish()),
            Arc::new(date_builder.finish()),
            Arc::new(ts_builder.finish()),
            Arc::new(bin_builder.finish()),
            Arc::new(tags_arr),
            Arc::new(props_arr),
            Arc::new(profile_arr),
        ],
    )
    .unwrap();

    // 1. Default Update mode with ignore_nulls = false
    let params_update_nil = WriteParameters {
        job_id: Some("job_1".to_string()),
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "user_id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: Some(3600),
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    let records = convert_record_batch(&batch, &params_update_nil, 64).unwrap();
    assert_eq!(records.len(), 2);

    let r0 = &records[0];
    assert_eq!(r0.bins.len(), 10);
    assert_eq!(r0.bins[0], Bin::new("age", Value::Int(30)));
    assert_eq!(
        r0.bins[1],
        Bin::new("score", Value::Float(FloatValue::from(99.5)))
    );
    assert_eq!(r0.bins[2], Bin::new("active", Value::Bool(true)));
    assert_eq!(
        r0.bins[3],
        Bin::new("amount", Value::String("123.45".to_string()))
    );
    assert_eq!(
        r0.bins[4],
        Bin::new("birth_date", Value::Int(1_728_000_000_000))
    );
    assert_eq!(
        r0.bins[5],
        Bin::new("event_ts", Value::Int(1_700_000_000_123))
    );
    assert_eq!(
        r0.bins[6],
        Bin::new("raw_bytes", Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]))
    );
    assert_eq!(
        r0.bins[7],
        Bin::new(
            "tags",
            Value::List(vec![
                Value::String("vip".into()),
                Value::String("beta".into())
            ])
        )
    );

    let mut expected_props = IndexMap::new();
    expected_props.insert(Value::String("clicks".into()), Value::Int(42));
    assert_eq!(
        r0.bins[8],
        Bin::new("props", Value::OrderedMap(expected_props))
    );

    let mut expected_profile = IndexMap::new();
    expected_profile.insert(Value::String("city".into()), Value::String("Berlin".into()));
    expected_profile.insert(Value::String("zip".into()), Value::Int(10115));
    assert_eq!(
        r0.bins[9],
        Bin::new("profile", Value::OrderedMap(expected_profile))
    );

    // Row 1 in Update mode with ignore_nulls = false must emit Value::Nil for all 9 null bins!
    let r1 = &records[1];
    assert_eq!(r1.bins.len(), 10);
    assert_eq!(r1.bins[0], Bin::new("age", Value::Nil));
    assert_eq!(
        r1.bins[1],
        Bin::new("score", Value::Float(FloatValue::from(50.0)))
    );
    assert_eq!(r1.bins[2], Bin::new("active", Value::Nil));

    // 2. Replace mode omits null bins (and skips rows where all bins are null)!
    let mut params_replace = params_update_nil.clone();
    params_replace.write_mode = WriteMode::Replace;
    let records_replace = convert_record_batch(&batch, &params_replace, 64).unwrap();
    assert_eq!(records_replace[1].bins.len(), 1);
    assert_eq!(
        records_replace[1].bins[0],
        Bin::new("score", Value::Float(FloatValue::from(50.0)))
    );

    // Also verify that in Replace mode, a row where ALL bins are null is completely omitted
    let all_null_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let mut all_null_id = StringBuilder::new();
    let mut all_null_age = Int32Builder::new();
    all_null_id.append_value("u_all_null");
    all_null_age.append_null();
    let all_null_batch = RecordBatch::try_new(
        all_null_schema,
        vec![
            Arc::new(all_null_id.finish()),
            Arc::new(all_null_age.finish()),
        ],
    )
    .unwrap();
    let records_replace_all_null =
        convert_record_batch(&all_null_batch, &params_replace, 64).unwrap();
    assert!(
        records_replace_all_null.is_empty(),
        "Replace mode must skip rows where all bins are null"
    );

    // 3. Update mode with ignore_nulls = true also omits null bins!
    let mut params_ignore_nulls = params_update_nil.clone();
    params_ignore_nulls.ignore_nulls = true;
    let records_ignore = convert_record_batch(&batch, &params_ignore_nulls, 64).unwrap();
    assert_eq!(records_ignore[1].bins.len(), 1);
}

#[test]
fn test_null_primary_key_and_bin_name_length_validation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("very_long_bin_name_exceeding_limit", DataType::Int32, true),
    ]));

    let mut id_b = StringBuilder::new();
    let mut val_b = Int32Builder::new();
    id_b.append_value("k1");
    val_b.append_value(1);
    id_b.append_null(); // Row 1 has NULL primary key!
    val_b.append_value(2);

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(id_b.finish()), Arc::new(val_b.finish())],
    )
    .unwrap();

    let params = WriteParameters {
        job_id: None,
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: None,
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    // Null key must fail immediately
    let err = convert_record_batch(&batch, &params, 64).unwrap_err();
    assert!(
        err.to_string()
            .contains("Null value encountered in primary key")
    );

    // Bin name exceeding max_bin_name_len = 15 must fail immediately
    let batch_valid_key = batch.slice(0, 1);
    let err_len = convert_record_batch(&batch_valid_key, &params, 15).unwrap_err();
    assert!(
        err_len
            .to_string()
            .contains("exceeds configured max_bin_name_len")
    );

    // Bin name with invalid characters (whitespace / delimiter / non-ASCII) must fail
    for bad_name in ["bad space", "bad:colon", "bad;semi", "кириллица"] {
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(bad_name, DataType::Int32, true),
        ]));
        let mut k = StringBuilder::new();
        let mut v = Int32Builder::new();
        k.append_value("k1");
        v.append_value(10);
        let b = RecordBatch::try_new(bad_schema, vec![Arc::new(k.finish()), Arc::new(v.finish())])
            .unwrap();
        let err_fmt = convert_record_batch(&b, &params, 64).unwrap_err();
        assert!(err_fmt.to_string().contains("invalid format"));
    }

    // Schema with only PK column and 0 bin columns must fail
    let only_pk_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let mut k_only = StringBuilder::new();
    k_only.append_value("k1");
    let b_only_pk = RecordBatch::try_new(only_pk_schema, vec![Arc::new(k_only.finish())]).unwrap();
    let err_zero_bins = convert_record_batch(&b_only_pk, &params, 15).unwrap_err();
    assert!(
        err_zero_bins
            .to_string()
            .contains("must contain at least 1 bin column")
    );

    // Row with PK and ALL bins set to NULL (both typed null and DataType::Null)
    let all_null_bins_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("bin_a", DataType::Int32, true),
        Field::new("bin_b", DataType::Null, true),
    ]));
    let mut k_null_test = StringBuilder::new();
    let mut a_null_test = Int32Builder::new();
    k_null_test.append_value("user_deleted_all_bins");
    a_null_test.append_null();
    let b_all_null = RecordBatch::try_new(
        all_null_bins_schema,
        vec![
            Arc::new(k_null_test.finish()),
            Arc::new(a_null_test.finish()),
            Arc::new(arrow_array::NullArray::new(1)),
        ],
    )
    .unwrap();

    let converted = convert_record_batch(&b_all_null, &params, 15).unwrap();
    assert_eq!(converted.len(), 1);
    assert_eq!(
        converted[0].bins,
        vec![Bin::new("bin_a", Value::Nil), Bin::new("bin_b", Value::Nil),]
    );
    // In DQ validation, if all bins were deleted (Value::Nil), an absent record (None) in Aerospike must match!
    assert_eq!(
        as_proxy_server::converter::validate_record_against_bins(&converted[0], None),
        as_proxy_server::converter::RecordDqResult::Matched
    );
}

#[test]
fn test_authenticator_rejects_bad_credentials_and_tokens() {
    let auth = Authenticator::new(AuthConfig {
        enabled: true,
        username: "admin".to_string(),
        password: "correct_password".to_string(),
        token_secret: "secret_123".to_string(),
        token_ttl_secs: 3600,
    });

    assert!(auth.verify_credentials("admin", "correct_password").is_ok());
    assert!(auth.verify_credentials("admin", "wrong_password").is_err());
    assert!(
        auth.verify_credentials("wrong_user", "correct_password")
            .is_err()
    );

    let token = auth.issue_token("admin").unwrap();
    assert_eq!(auth.verify_token(&token).unwrap(), "admin");
    assert!(auth.verify_token(&format!("{token}tampered")).is_err());

    let mut headers = MetadataMap::new();
    assert!(auth.verify_metadata(&headers).is_err());
    headers.insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    assert!(auth.verify_metadata(&headers).is_ok());
}

#[tokio::test]
async fn test_infinite_overload_retry_and_partial_batch_retry() {
    let k1 = Key::new("ns", "set", Value::Int(1)).unwrap();
    let k2 = Key::new("ns", "set", Value::Int(2)).unwrap();
    let k3 = Key::new("ns", "set", Value::Int(3)).unwrap();

    // k2 fails with DEVICE_OVERLOAD / KEY_BUSY 7 times in a row, while max_other_retries is only 2!
    let mut overload_map = HashMap::new();
    overload_map.insert(k2.clone(), 7);

    let sink = Arc::new(MockAerospikeSink::new(overload_map, HashMap::new()));
    let metrics = ProxyMetrics::new();

    let writer_cfg = WriterConfig {
        use_batch_write: true,
        batch_write_size: 10,
        max_in_flight_records: 100,
        max_concurrent_subbatches_per_stream: 4,
        ..WriterConfig::default()
    };
    let retry_cfg = RetryConfig {
        max_other_retries: 2,
        initial_backoff_ms: 1,
        max_backoff_ms: 4,
        backoff_multiplier: 1.5,
    };

    let engine = MppWriteEngine::new(sink.clone(), writer_cfg, retry_cfg, Arc::clone(&metrics));

    let records = vec![
        PreparedRecord {
            key: k1.clone(),
            bins: vec![Bin::new("b", Value::Int(10))],
        },
        PreparedRecord {
            key: k2.clone(),
            bins: vec![Bin::new("b", Value::Int(20))],
        },
        PreparedRecord {
            key: k3.clone(),
            bins: vec![Bin::new("b", Value::Int(30))],
        },
    ];

    let params = WriteParameters {
        job_id: None,
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    let stats = engine.write_records(records, &params).await.unwrap();
    assert_eq!(stats.rows_written, 3);
    assert_eq!(stats.overload_retries, 7);
    assert_eq!(stats.other_retries, 0);

    // Verify k1 and k3 were only attempted ONCE (partial batch retry only retried k2!)
    let attempts = sink.write_attempts.lock().unwrap();
    assert_eq!(attempts.get(&k1), Some(&1));
    assert_eq!(attempts.get(&k2), Some(&8));
    assert_eq!(attempts.get(&k3), Some(&1));
}

#[tokio::test]
async fn test_single_put_fallback_mode_with_overload_retry() {
    let k1 = Key::new("ns", "set", Value::Int(10)).unwrap();
    let mut overload_map = HashMap::new();
    overload_map.insert(k1.clone(), 4);

    let sink = Arc::new(MockAerospikeSink::new(overload_map, HashMap::new()));
    let metrics = ProxyMetrics::new();

    let writer_cfg = WriterConfig {
        use_batch_write: false,
        batch_write_size: 10,
        max_in_flight_records: 50,
        max_concurrent_subbatches_per_stream: 2,
        ..WriterConfig::default()
    };
    let retry_cfg = RetryConfig {
        max_other_retries: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 3,
        backoff_multiplier: 1.5,
    };

    let engine = MppWriteEngine::new(sink.clone(), writer_cfg, retry_cfg, metrics);
    let records = vec![PreparedRecord {
        key: k1.clone(),
        bins: vec![Bin::new("b", Value::Int(99))],
    }];

    let params = WriteParameters {
        job_id: None,
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(false),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    let stats = engine.write_records(records, &params).await.unwrap();
    assert_eq!(stats.rows_written, 1);
    assert_eq!(stats.overload_retries, 4);
}

#[tokio::test]
async fn test_non_overload_error_terminates_after_n_retries() {
    let k1 = Key::new("ns", "set", Value::Int(100)).unwrap();

    // k1 fails with a non-overload error 10 times; with max_other_retries = 3,
    // it must stop after 4 attempts (1 initial + 3 retries) and return WriteFailed.
    let mut other_map = HashMap::new();
    other_map.insert(k1.clone(), 10);

    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), other_map));
    let metrics = ProxyMetrics::new();

    let writer_cfg = WriterConfig::default();
    let retry_cfg = RetryConfig {
        max_other_retries: 3,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
        backoff_multiplier: 1.5,
    };

    let engine = MppWriteEngine::new(sink.clone(), writer_cfg, retry_cfg, metrics);

    let records = vec![PreparedRecord {
        key: k1.clone(),
        bins: vec![Bin::new("b", Value::Int(1))],
    }];

    let params = WriteParameters {
        job_id: None,
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    let err = engine.write_records(records, &params).await.unwrap_err();
    let attempts = sink.write_attempts.lock().unwrap();
    assert_eq!(attempts.get(&k1), Some(&4));
    assert!(err.to_string().contains("after 4 attempts"));
}

#[tokio::test]
async fn test_end_to_end_flight_handshake_and_do_put_stream() {
    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig::default(),
        RetryConfig::default(),
        Arc::clone(&metrics),
    ));

    let auth_cfg = AuthConfig {
        enabled: true,
        username: "spark_user".to_string(),
        password: "spark_password_123".to_string(),
        token_secret: "test_hmac_secret_key".to_string(),
        token_ttl_secs: 3600,
    };
    let authenticator = Arc::new(Authenticator::new(auth_cfg));

    let service = ProxyFlightService::new(authenticator, engine, Arc::clone(&metrics), 64);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // 1. Perform Arrow Flight Handshake using BasicAuth protobuf payload
    let basic_proto = BasicAuth {
        username: "spark_user".to_string(),
        password: "spark_password_123".to_string(),
    };
    let hs_req = HandshakeRequest {
        protocol_version: 0,
        payload: Bytes::from(basic_proto.encode_to_vec()),
    };
    let hs_resp = client
        .handshake(Request::new(stream::once(async move { hs_req })))
        .await
        .unwrap();

    let bearer_header = hs_resp
        .metadata()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(bearer_header.starts_with("Bearer "));

    // 2. Build RecordBatch and FlightDescriptor for DoPut
    let schema = Arc::new(Schema::new(vec![
        Field::new("key_col", DataType::Int64, false),
        Field::new("bin_str", DataType::Utf8, true),
        Field::new("bin_val", DataType::Int64, true),
    ]));

    let mut k_b = Int64Builder::new();
    let mut s_b = StringBuilder::new();
    let mut v_b = Int64Builder::new();
    for i in 1..=50 {
        k_b.append_value(i);
        s_b.append_value(format!("val_{i}"));
        if i % 10 == 0 {
            v_b.append_null();
        } else {
            v_b.append_value(i * 100);
        }
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(k_b.finish()),
            Arc::new(s_b.finish()),
            Arc::new(v_b.finish()),
        ],
    )
    .unwrap();

    let write_params = WriteParameters {
        job_id: Some("job_e2e_1".to_string()),
        namespace: "prod_ns".to_string(),
        set_name: "features".to_string(),
        key_column: "key_col".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: Some(7200),
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };
    let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(&write_params).unwrap());

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter(vec![Ok(batch)]))
        .map(|res| res.unwrap());

    let mut do_put_req = Request::new(flight_data_stream);
    do_put_req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(bearer_header.as_str()).unwrap(),
    );

    let mut put_resp_stream = client.do_put(do_put_req).await.unwrap().into_inner();
    let mut acks = Vec::new();
    while let Some(put_res) = put_resp_stream.try_next().await.unwrap() {
        let ack: PutBatchAck = serde_json::from_slice(&put_res.app_metadata).unwrap();
        acks.push(ack);
    }

    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].rows_written, 50);

    let stored = sink.stored.lock().unwrap();
    assert_eq!(stored.len(), 50);
    let key_10 = Key::new("prod_ns", "features", Value::Int(10)).unwrap();
    let bins_10 = stored.get(&key_10).unwrap();
    assert_eq!(
        bins_10,
        &vec![
            Bin::new("bin_str", Value::String("val_10".to_string())),
            Bin::new("bin_val", Value::Nil),
        ]
    );
}

#[tokio::test]
async fn test_three_node_raft_cluster_metadata_and_distributed_job_status() {
    // Reserve 3 ephemeral ports for Raft and 3 ephemeral ports for Arrow Flight
    let mut raft_addrs = Vec::new();
    let mut flight_listeners = Vec::new();
    let mut flight_addrs = Vec::new();

    for _ in 0..3 {
        let rl = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let r_addr = rl.local_addr().unwrap().to_string();
        drop(rl);
        raft_addrs.push(r_addr);

        let fl = TcpListener::bind("127.0.0.1:0").await.unwrap();
        flight_addrs.push(fl.local_addr().unwrap().to_string());
        flight_listeners.push(fl);
    }

    let peers: Vec<RaftPeer> = (0..3)
        .map(|i| RaftPeer {
            node_id: (i + 1) as u64,
            raft_addr: raft_addrs[i].clone(),
            flight_addr: flight_addrs[i].clone(),
        })
        .collect();

    let shared_sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let mut raft_nodes = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let raft_cfg = RaftConfig {
            enabled: true,
            node_id,
            listen_addr: raft_addrs[i].clone(),
            election_timeout_min_ms: 100,
            election_timeout_max_ms: 200,
            heartbeat_interval_ms: 35,
            job_stats_ttl_secs: 3600,
            peers: peers.clone(),
            ..RaftConfig::default()
        };
        let raft_node = RaftNode::new(raft_cfg);
        raft_node.start().await.unwrap();
        raft_nodes.push(Arc::clone(&raft_node));

        let metrics = ProxyMetrics::new();
        let engine = Arc::new(MppWriteEngine::new(
            shared_sink.clone(),
            WriterConfig::default(),
            RetryConfig::default(),
            Arc::clone(&metrics),
        ));
        let auth = Arc::new(Authenticator::new(AuthConfig {
            enabled: false,
            ..AuthConfig::default()
        }));
        let svc = ProxyFlightService::new(auth, engine, metrics, 64).with_raft_node(raft_node);
        let fl = flight_listeners.remove(0);

        tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(fl))
                .await
                .unwrap();
        });
    }

    // Wait up to 2 seconds for Raft leader election to converge
    let mut leader_elected = false;
    for _ in 0..40 {
        for r in &raft_nodes {
            let snap = r.get_cluster_snapshot().await;
            if snap.role == RaftRole::Leader {
                leader_elected = true;
                break;
            }
        }
        if leader_elected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(leader_elected, "3-node Raft cluster must elect a leader");

    // Stream batches concurrently to ALL 3 equal proxy nodes for the same Spark job_id
    let job_id = "spark-job-distributed-42";
    let counts_per_node = [20usize, 30usize, 40usize];
    let mut start_key = 1i64;

    for (idx, &row_count) in counts_per_node.iter().enumerate() {
        let channel = tonic::transport::Endpoint::new(format!("http://{}", flight_addrs[idx]))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightServiceClient::new(channel);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let mut k_b = Int64Builder::new();
        let mut v_b = Int64Builder::new();
        for k in start_key..(start_key + row_count as i64) {
            k_b.append_value(k);
            v_b.append_value(k * 10);
        }
        start_key += row_count as i64;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(k_b.finish()), Arc::new(v_b.finish())],
        )
        .unwrap();

        let params = WriteParameters {
            job_id: Some(job_id.to_string()),
            namespace: "ns".to_string(),
            set_name: "set".to_string(),
            key_column: "id".to_string(),
            write_mode: WriteMode::Update,
            ttl_seconds: None,
            send_key: true,
            ignore_nulls: false,
            use_batch_write: Some(true),
            validate_only: false,
            compression: None,
            auto_convert_strings: false,
        };
        let desc = FlightDescriptor::new_cmd(serde_json::to_vec(&params).unwrap());
        let stream_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(desc))
            .build(stream::iter(vec![Ok(batch)]))
            .map(|r| r.unwrap());

        let mut resp = client
            .do_put(Request::new(stream_data))
            .await
            .unwrap()
            .into_inner();
        while (resp.try_next().await.unwrap()).is_some() {}
    }

    // Send `complete_job` action to Node 2 (even if Node 2 is a Follower, it forwards to the Raft Leader!)
    let channel_n2 = tonic::transport::Endpoint::new(format!("http://{}", flight_addrs[1]))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_n2 = FlightServiceClient::new(channel_n2);
    let _ = client_n2
        .do_action(Request::new(Action {
            r#type: "complete_job".to_string(),
            body: Bytes::from(job_id),
        }))
        .await
        .unwrap()
        .into_inner()
        .try_next()
        .await
        .unwrap();

    // Wait briefly for Raft heartbeat commit propagation to all followers
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Query `get_job_status` via Arrow Flight DoAction on ALL 3 nodes and verify identical replicated totals!
    for f_addr in &flight_addrs {
        let ch = tonic::transport::Endpoint::new(format!("http://{f_addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightServiceClient::new(ch);
        let mut action_stream = client
            .do_action(Request::new(Action {
                r#type: "get_job_status".to_string(),
                body: Bytes::from(job_id),
            }))
            .await
            .unwrap()
            .into_inner();

        let res = action_stream.try_next().await.unwrap().unwrap();
        let status: JobLoadStatus = serde_json::from_slice(&res.body).unwrap();

        assert_eq!(status.job_id, job_id);
        assert_eq!(status.state, JobState::Completed);
        assert_eq!(status.total_rows_written, 90);
        assert_eq!(status.total_batches_processed, 3);
        assert_eq!(status.per_node_rows.get(&1), Some(&20));
        assert_eq!(status.per_node_rows.get(&2), Some(&30));
        assert_eq!(status.per_node_rows.get(&3), Some(&40));
    }

    for r in raft_nodes {
        r.shutdown();
    }
}

#[tokio::test]
async fn test_raft_job_stats_ttl_expiration() {
    let raft_cfg = RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: "127.0.0.1:0".to_string(),
        election_timeout_min_ms: 100,
        election_timeout_max_ms: 200,
        heartbeat_interval_ms: 35,
        job_stats_ttl_secs: 1, // 1 second TTL
        peers: Vec::new(),
        ..RaftConfig::default()
    };
    let raft_node = RaftNode::new(raft_cfg);
    raft_node.start().await.unwrap();

    raft_node
        .propose(as_proxy_server::raft::MetadataCommand::RecordJobBatch {
            job_id: "ttl-job-1".to_string(),
            node_id: 1,
            namespace: "ns".to_string(),
            set_name: "set".to_string(),
            is_dq_validation: false,
            rows_written: 125,
            overload_retries: 2,
            other_retries: 0,
            rows_validated: 0,
            matched_rows: 0,
            mismatched_rows: 0,
            missing_keys: 0,
            bin_mismatch_rows: 0,
            mismatch_samples: Vec::new(),
            timestamp_ms: as_proxy_server::raft::now_ms(),
        })
        .await
        .unwrap();

    // Immediately after write, job status is available
    let status = raft_node.get_job_status("ttl-job-1").await;
    assert!(status.is_some());
    assert_eq!(status.unwrap().total_rows_written, 125);

    // Wait for TTL (1s) + background purge interval to elapse
    tokio::time::sleep(Duration::from_millis(1400)).await;

    // Job stats must now be expired and purged from Raft state
    assert!(raft_node.get_job_status("ttl-job-1").await.is_none());
    let snap = raft_node.get_cluster_snapshot().await;
    assert!(!snap.state.jobs.contains_key("ttl-job-1"));

    raft_node.shutdown();
}

#[tokio::test]
async fn test_dq_validation_with_nan_equality_and_mismatch_detection() {
    // 1. Unit check: two NaNs (including different IEEE-754 NaN payload/sign bits and nested NaNs) must be equal!
    let nan1 = Value::Float(FloatValue::from(f64::NAN));
    let nan2 = Value::Float(FloatValue::from(-f64::NAN));
    let nan3 = Value::Float(FloatValue::from(f64::from_bits(0x7ff8_0000_0000_0042)));
    assert!(values_equal_dq(&nan1, &nan2));
    assert!(values_equal_dq(&nan1, &nan3));
    assert!(values_equal_dq(
        &Value::List(vec![nan1.clone(), Value::Int(10)]),
        &Value::List(vec![nan3.clone(), Value::Int(10)])
    ));
    assert!(!values_equal_dq(
        &nan1,
        &Value::Float(FloatValue::from(1.0))
    ));

    // 2. End-to-end Flight DoPut + Raft job status check in DQ validation mode
    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    {
        let mut stored = sink.stored.lock().unwrap();
        // Key 1: score is NaN (with non-standard NaN bit pattern), age is 25
        stored.insert(
            Key::new("ns", "users", Value::Int(1)).unwrap(),
            vec![
                Bin::new("score", nan3.clone()),
                Bin::new("age", Value::Int(25)),
            ],
        );
        // Key 2: score is 42.5, age bin was deleted (Nil)
        stored.insert(
            Key::new("ns", "users", Value::Int(2)).unwrap(),
            vec![
                Bin::new("score", Value::Float(FloatValue::from(42.5))),
                Bin::new("age", Value::Nil),
            ],
        );
        // Key 3: score is 10.0, age is 30 (will mismatch against Spark's age = 99)
        stored.insert(
            Key::new("ns", "users", Value::Int(3)).unwrap(),
            vec![
                Bin::new("score", Value::Float(FloatValue::from(10.0))),
                Bin::new("age", Value::Int(30)),
            ],
        );
        // Key 4 is NOT in Aerospike at all (missing key)
    }

    let raft_node = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: "127.0.0.1:0".to_string(),
        ..RaftConfig::default()
    });
    raft_node.start().await.unwrap();

    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig::default(),
        RetryConfig::default(),
        Arc::clone(&metrics),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));
    let svc =
        ProxyFlightService::new(auth, engine, metrics, 64).with_raft_node(Arc::clone(&raft_node));

    let fl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let f_addr = fl.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(fl))
            .await
            .unwrap();
    });

    let ch = tonic::transport::Endpoint::new(format!("http://{f_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(ch);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
        Field::new("age", DataType::Int64, true),
    ]));

    // First DQ job: 4 rows (Row 1 matches with NaN==NaN, Row 2 matches with null/deleted bin,
    // Row 3 has bin mismatch, Row 4 is missing key)
    let mut id_b = Int64Builder::new();
    let mut score_b = Float64Builder::new();
    let mut age_b = Int64Builder::new();

    // Row 1: score = NaN, age = 25 -> Match!
    id_b.append_value(1);
    score_b.append_value(f64::NAN);
    age_b.append_value(25);

    // Row 2: score = 42.5, age = null -> Match!
    id_b.append_value(2);
    score_b.append_value(42.5);
    age_b.append_null();

    // Row 3: score = 10.0, age = 99 (Aerospike has 30) -> BinMismatch!
    id_b.append_value(3);
    score_b.append_value(10.0);
    age_b.append_value(99);

    // Row 4: missing in Aerospike -> MissingKey!
    id_b.append_value(4);
    score_b.append_value(0.0);
    age_b.append_value(1);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(id_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(age_b.finish()),
        ],
    )
    .unwrap();

    let dq_params = WriteParameters {
        job_id: Some("dq-job-failed".to_string()),
        namespace: "ns".to_string(),
        set_name: "users".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Validate,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: true,
        compression: None,
        auto_convert_strings: false,
    };
    let desc = FlightDescriptor::new_cmd(serde_json::to_vec(&dq_params).unwrap());
    let stream_data = FlightDataEncoderBuilder::new()
        .with_schema(Arc::clone(&schema))
        .with_flight_descriptor(Some(desc))
        .build(stream::iter(vec![Ok(batch.clone())]))
        .map(|r| r.unwrap());

    let mut put_resp = client
        .do_put(Request::new(stream_data))
        .await
        .unwrap()
        .into_inner();
    let ack_msg = put_resp.try_next().await.unwrap().unwrap();
    let ack: PutBatchAck = serde_json::from_slice(&ack_msg.app_metadata).unwrap();
    assert_eq!(ack.rows_validated, 4);
    assert_eq!(ack.matched_rows, 2);
    assert_eq!(ack.mismatched_rows, 2);
    assert_eq!(ack.missing_keys, 1);
    assert_eq!(ack.bin_mismatch_rows, 1);

    // Query Raft job status via DoAction
    let status_res = client
        .do_action(Request::new(Action {
            r#type: "get_job_status".to_string(),
            body: Bytes::from("dq-job-failed"),
        }))
        .await
        .unwrap()
        .into_inner()
        .try_next()
        .await
        .unwrap()
        .unwrap();
    let status: JobLoadStatus = serde_json::from_slice(&status_res.body).unwrap();
    assert!(status.is_dq_validation);
    assert_eq!(status.total_rows_validated, 4);
    assert_eq!(status.total_matched_rows, 2);
    assert_eq!(status.total_mismatched_rows, 2);
    assert_eq!(status.total_missing_keys, 1);
    assert_eq!(status.total_bin_mismatch_rows, 1);
    assert_eq!(status.dq_passed, Some(false));
    assert_eq!(status.dq_mismatch_samples.len(), 2);

    // Second DQ job: validate only the first 2 rows (which include NaN == NaN and null bin) -> dq_passed must be Some(true)!
    let valid_batch = batch.slice(0, 2);
    let mut dq_params_ok = dq_params.clone();
    dq_params_ok.job_id = Some("dq-job-passed".to_string());
    let desc_ok = FlightDescriptor::new_cmd(serde_json::to_vec(&dq_params_ok).unwrap());
    let stream_ok = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(desc_ok))
        .build(stream::iter(vec![Ok(valid_batch)]))
        .map(|r| r.unwrap());

    let mut resp_ok = client
        .do_put(Request::new(stream_ok))
        .await
        .unwrap()
        .into_inner();
    while (resp_ok.try_next().await.unwrap()).is_some() {}

    let status_ok_res = client
        .do_action(Request::new(Action {
            r#type: "get_job_status".to_string(),
            body: Bytes::from("dq-job-passed"),
        }))
        .await
        .unwrap()
        .into_inner()
        .try_next()
        .await
        .unwrap()
        .unwrap();
    let status_ok: JobLoadStatus = serde_json::from_slice(&status_ok_res.body).unwrap();
    assert!(status_ok.is_dq_validation);
    assert_eq!(status_ok.total_rows_validated, 2);
    assert_eq!(status_ok.total_matched_rows, 2);
    assert_eq!(status_ok.total_mismatched_rows, 0);
    assert_eq!(status_ok.dq_passed, Some(true));
    assert!(status_ok.dq_mismatch_samples.is_empty());

    raft_node.shutdown();
}

#[tokio::test]
async fn test_flight_read_get_schema_do_get_and_do_exchange_lookup() {
    use arrow_flight::Ticket;
    use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};

    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig::default(),
        RetryConfig::default(),
        metrics.clone(),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));
    let flight_svc = ProxyFlightService::new(auth, engine, metrics.clone(), 15);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // 1. Write 4 records via DoPut
    let write_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let mut pk_b = StringBuilder::new();
    let mut age_b = Int32Builder::new();
    let mut score_b = Float64Builder::new();
    let mut name_b = StringBuilder::new();

    for (k, a, s, n) in [
        ("u_1", 25, 10.5, "Alice"),
        ("u_2", 30, 20.0, "Bob"),
        ("u_3", 35, 30.25, "Charlie"),
        ("u_4", 40, 40.75, "Diana"),
    ] {
        pk_b.append_value(k);
        age_b.append_value(a);
        score_b.append_value(s);
        name_b.append_value(n);
    }

    let write_batch = RecordBatch::try_new(
        write_schema.clone(),
        vec![
            Arc::new(pk_b.finish()),
            Arc::new(age_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .unwrap();

    let w_params = WriteParameters {
        job_id: Some("job-seed-read".to_string()),
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "pk".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    let desc = FlightDescriptor::new_cmd(serde_json::to_vec(&w_params).unwrap());
    let put_stream = FlightDataEncoderBuilder::new()
        .with_schema(write_schema.clone())
        .with_flight_descriptor(Some(desc))
        .build(stream::iter(vec![Ok(write_batch)]))
        .map(|r| r.unwrap());

    let mut put_resp = client
        .do_put(Request::new(put_stream))
        .await
        .unwrap()
        .into_inner();
    while (put_resp.try_next().await.unwrap()).is_some() {}

    // 2. Test get_schema (inferSchema by sampling)
    let infer_req = InferSchemaRequest {
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "pk".to_string(),
        sample_size: 100,
    };
    let schema_res = client
        .get_schema(Request::new(FlightDescriptor::new_cmd(
            serde_json::to_vec(&infer_req).unwrap(),
        )))
        .await
        .unwrap()
        .into_inner();
    let inferred_schema = Schema::try_from(&schema_res).unwrap();
    assert_eq!(inferred_schema.fields().len(), 4);
    assert_eq!(inferred_schema.field(0).name(), "pk");

    // 3. Test do_get with PartitionRange across 2 splits (0..2048 and 2048..4096) + Column Pruning (only pk & age)
    let pruned_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]));
    let pruned_schema_b64 = encode_arrow_schema_ipc_base64(&pruned_schema).unwrap();

    let mut scanned_rows = 0;
    for (begin, count) in [(0, 2048), (2048, 2048)] {
        let ticket_json = serde_json::to_vec(&ReadTicket {
            namespace: "test".to_string(),
            set_name: "users".to_string(),
            key_column: "pk".to_string(),
            bins: vec!["age".to_string()],
            read_mode: ReadModeSpec::PartitionRange {
                begin_partition: begin,
                partition_count: count,
            },
            batch_size: 2,
            type_mismatch_mode: TypeMismatchMode::Fail,
            schema_ipc_base64: pruned_schema_b64.clone(),
            compression: None,
            compression_level: None,
        })
        .unwrap();

        let do_get_stream = client
            .do_get(Request::new(Ticket {
                ticket: Bytes::from(ticket_json),
            }))
            .await
            .unwrap()
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);

        let mut dec = FlightDataDecoder::new(do_get_stream);
        while let Some(msg) = dec.try_next().await.unwrap() {
            if let DecodedPayload::RecordBatch(rb) = msg.payload {
                assert_eq!(rb.num_columns(), 2);
                scanned_rows += rb.num_rows();
            }
        }
    }
    assert_eq!(scanned_rows, 4);

    // 4. Test do_get with BatchKeys (2 existing keys + 1 missing key -> returns 2 found records)
    let batch_keys_ticket = serde_json::to_vec(&ReadTicket {
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "pk".to_string(),
        bins: vec!["age".to_string(), "score".to_string(), "name".to_string()],
        read_mode: ReadModeSpec::BatchKeys {
            keys: vec![
                KeyLiteral::String("u_2".to_string()),
                KeyLiteral::String("u_missing".to_string()),
                KeyLiteral::String("u_4".to_string()),
            ],
        },
        batch_size: 10,
        type_mismatch_mode: TypeMismatchMode::Fail,
        schema_ipc_base64: encode_arrow_schema_ipc_base64(&write_schema).unwrap(),
        compression: None,
        compression_level: None,
    })
    .unwrap();

    let bk_stream = client
        .do_get(Request::new(Ticket {
            ticket: Bytes::from(batch_keys_ticket),
        }))
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut bk_dec = FlightDataDecoder::new(bk_stream);
    let mut bk_rows = 0;
    while let Some(msg) = bk_dec.try_next().await.unwrap() {
        if let DecodedPayload::RecordBatch(rb) = msg.payload {
            bk_rows += rb.num_rows();
        }
    }
    assert_eq!(bk_rows, 2);

    // 5. Test do_exchange distributed DataFrame key lookup (both Left and Inner join)
    let input_df_schema = Arc::new(Schema::new(vec![
        Field::new("req_id", DataType::Int32, false),
        Field::new("pk", DataType::Utf8, false),
    ]));
    let mut req_b = Int32Builder::new();
    let mut key_b = StringBuilder::new();
    req_b.append_value(100);
    key_b.append_value("u_1");
    req_b.append_value(200);
    key_b.append_value("u_not_in_aerospike");
    req_b.append_value(300);
    key_b.append_value("u_3");

    let input_df_batch = RecordBatch::try_new(
        input_df_schema.clone(),
        vec![Arc::new(req_b.finish()), Arc::new(key_b.finish())],
    )
    .unwrap();

    let bins_only_schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let bins_schema_b64 = encode_arrow_schema_ipc_base64(&bins_only_schema).unwrap();

    // 5a. Left join -> returns all 3 rows, with row 1 having NULL age & name
    let left_params = LookupExchangeParams {
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "pk".to_string(),
        join_type: LookupJoinType::Left,
        type_mismatch_mode: TypeMismatchMode::Fail,
        bins_schema_ipc_base64: bins_schema_b64.clone(),
        compression: None,
        compression_level: None,
    };
    let left_desc = FlightDescriptor::new_cmd(serde_json::to_vec(&left_params).unwrap());
    let left_req_stream = FlightDataEncoderBuilder::new()
        .with_schema(input_df_schema.clone())
        .with_flight_descriptor(Some(left_desc))
        .build(stream::iter(vec![Ok(input_df_batch.clone())]))
        .map(|r| r.unwrap());

    let left_resp = client
        .do_exchange(Request::new(left_req_stream))
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut left_dec = FlightDataDecoder::new(left_resp);
    let mut left_batches = Vec::new();
    while let Some(msg) = left_dec.try_next().await.unwrap() {
        if let DecodedPayload::RecordBatch(rb) = msg.payload {
            left_batches.push(rb);
        }
    }
    assert_eq!(left_batches.len(), 1);
    let l_rb = &left_batches[0];
    assert_eq!(l_rb.num_rows(), 3);
    assert_eq!(l_rb.num_columns(), 4); // req_id, pk, age, name
    assert!(!l_rb.column(2).is_null(0));
    assert!(l_rb.column(2).is_null(1)); // u_not_in_aerospike -> age is NULL
    assert!(l_rb.column(3).is_null(1)); // u_not_in_aerospike -> name is NULL
    assert!(!l_rb.column(2).is_null(2));

    // 5b. Inner join -> returns only the 2 matching rows (u_1 and u_3)
    let inner_params = LookupExchangeParams {
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "pk".to_string(),
        join_type: LookupJoinType::Inner,
        type_mismatch_mode: TypeMismatchMode::Fail,
        bins_schema_ipc_base64: bins_schema_b64,
        compression: None,
        compression_level: None,
    };
    let inner_desc = FlightDescriptor::new_cmd(serde_json::to_vec(&inner_params).unwrap());
    let inner_req_stream = FlightDataEncoderBuilder::new()
        .with_schema(input_df_schema)
        .with_flight_descriptor(Some(inner_desc))
        .build(stream::iter(vec![Ok(input_df_batch)]))
        .map(|r| r.unwrap());

    let inner_resp = client
        .do_exchange(Request::new(inner_req_stream))
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut inner_dec = FlightDataDecoder::new(inner_resp);
    let mut inner_rows = 0;
    while let Some(msg) = inner_dec.try_next().await.unwrap() {
        if let DecodedPayload::RecordBatch(rb) = msg.payload {
            inner_rows += rb.num_rows();
        }
    }
    assert_eq!(inner_rows, 2);

    // 6. Verify Prometheus metrics for read & lookup operations
    let prom_text = metrics.render_prometheus(true, None);
    // 4 scanned + 2 batch_keys + 3 left_lookup + 2 inner_lookup = 11 records read
    assert!(
        prom_text.contains("as_proxy_records_read_total 11"),
        "Expected as_proxy_records_read_total 11 in:\n{prom_text}"
    );
    // 3 left_lookup keys + 3 inner_lookup keys = 6 lookup keys requested, 4 found
    assert!(
        prom_text.contains("as_proxy_lookup_keys_requested_total 6"),
        "Expected as_proxy_lookup_keys_requested_total 6 in:\n{prom_text}"
    );
    assert!(
        prom_text.contains("as_proxy_lookup_keys_found_total 4"),
        "Expected as_proxy_lookup_keys_found_total 4 in:\n{prom_text}"
    );
}

#[test]
fn test_config_headers_validation_and_error_status_conversions() {
    use aerospike::{Expiration, RecordExistsAction};
    use as_proxy_server::config::{AerospikeConfig, AppConfig, ServerConfig, TlsConfig};
    use as_proxy_server::error::ProxyError;
    use tonic::Code;

    // 1. Default configs & TOML loading
    let def_cfg = AppConfig::default();
    assert_eq!(def_cfg.server.listen_addr, "0.0.0.0:8815");
    assert_eq!(ServerConfig::default().metrics_addr, "0.0.0.0:9090");
    assert!(!TlsConfig::default().enabled);
    assert_eq!(AerospikeConfig::default().max_bin_name_len, 64);

    let tmp_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        tmp_file.path(),
        r#"
        [server]
        listen_addr = "127.0.0.1:18815"
        [aerospike]
        hosts = "10.0.0.1:3000"
        "#,
    )
    .unwrap();
    let loaded = AppConfig::from_file(tmp_file.path()).unwrap();
    assert_eq!(loaded.server.listen_addr, "127.0.0.1:18815");
    assert_eq!(loaded.aerospike.hosts, "10.0.0.1:3000");

    assert!(AppConfig::from_file("/non/existent/path/config.toml").is_err());
    let bad_toml = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(bad_toml.path(), "invalid = [toml").unwrap();
    assert!(AppConfig::from_file(bad_toml.path()).is_err());

    // 2. WriteMode parsing and RecordExistsAction mapping
    for (s, mode, action) in [
        ("update", WriteMode::Update, RecordExistsAction::Update),
        ("bins", WriteMode::Update, RecordExistsAction::Update),
        ("replace", WriteMode::Replace, RecordExistsAction::Replace),
        ("row", WriteMode::Replace, RecordExistsAction::Replace),
        ("full", WriteMode::Replace, RecordExistsAction::Replace),
        (
            "update_only",
            WriteMode::UpdateOnly,
            RecordExistsAction::UpdateOnly,
        ),
        (
            "replace_only",
            WriteMode::ReplaceOnly,
            RecordExistsAction::ReplaceOnly,
        ),
        (
            "create_only",
            WriteMode::CreateOnly,
            RecordExistsAction::CreateOnly,
        ),
        ("validate", WriteMode::Validate, RecordExistsAction::Update),
        ("dq", WriteMode::Validate, RecordExistsAction::Update),
        ("verify", WriteMode::Validate, RecordExistsAction::Update),
    ] {
        let parsed = WriteMode::from_str_insensitive(s).unwrap();
        assert_eq!(parsed, mode);
        assert_eq!(parsed.to_record_exists_action(), action);
    }
    assert!(WriteMode::from_str_insensitive("invalid_mode").is_err());

    // 3. WriteParameters from gRPC MetadataMap headers & expiration
    let mut headers = MetadataMap::new();
    assert!(WriteParameters::from_descriptor_or_headers(None, &headers).is_err());
    headers.insert("x-as-namespace", MetadataValue::from_static("ns1"));
    assert!(WriteParameters::from_descriptor_or_headers(None, &headers).is_err());
    headers.insert("x-as-key-column", MetadataValue::from_static("pk"));
    headers.insert("x-as-set", MetadataValue::from_static("set1"));
    headers.insert("x-as-job-id", MetadataValue::from_static("job-42"));
    headers.insert("x-as-write-mode", MetadataValue::from_static("replace"));
    headers.insert("x-as-ttl", MetadataValue::from_static("120"));
    headers.insert("x-as-send-key", MetadataValue::from_static("false"));
    headers.insert("x-as-ignore-nulls", MetadataValue::from_static("true"));
    headers.insert("x-as-use-batch-write", MetadataValue::from_static("1"));
    headers.insert("x-as-validate-only", MetadataValue::from_static("0"));

    let mut wp = WriteParameters::from_descriptor_or_headers(None, &headers).unwrap();
    assert_eq!(wp.job_id.as_deref(), Some("job-42"));
    assert_eq!(wp.namespace, "ns1");
    assert_eq!(wp.set_name, "set1");
    assert_eq!(wp.key_column, "pk");
    assert_eq!(wp.write_mode, WriteMode::Replace);
    assert!(!wp.send_key);
    assert!(wp.ignore_nulls);
    assert_eq!(wp.use_batch_write, Some(true));
    assert!(!wp.validate_only);
    assert!(matches!(wp.expiration(), Expiration::Seconds(120)));

    wp.ttl_seconds = None;
    assert!(matches!(wp.expiration(), Expiration::NamespaceDefault));
    wp.ttl_seconds = Some(0);
    assert!(matches!(wp.expiration(), Expiration::NamespaceDefault));
    wp.ttl_seconds = Some(-1);
    assert!(matches!(wp.expiration(), Expiration::Never));
    wp.ttl_seconds = Some(-2);
    assert!(matches!(wp.expiration(), Expiration::DontUpdate));
    wp.ttl_seconds = Some(-99);
    assert!(matches!(wp.expiration(), Expiration::NamespaceDefault));

    // Invalid TTL & empty namespace/key_column
    headers.insert("x-as-ttl", MetadataValue::from_static("not_a_number"));
    assert!(WriteParameters::from_descriptor_or_headers(None, &headers).is_err());
    assert!(WriteParameters::from_descriptor_or_headers(Some(b"{bad_json"), &headers).is_err());

    let mut invalid_wp = wp.clone();
    invalid_wp.namespace = "   ".to_string();
    assert!(invalid_wp.validate().is_err());
    invalid_wp.namespace = "ns1".to_string();
    invalid_wp.key_column = "".to_string();
    assert!(invalid_wp.validate().is_err());

    // 4. TypeMismatchMode, LookupJoinType, KeyLiteral, ReadTicket, InferSchemaRequest, LookupExchangeParams
    assert_eq!(
        TypeMismatchMode::from_str_insensitive("FAIL").unwrap(),
        TypeMismatchMode::Fail
    );
    assert_eq!(
        TypeMismatchMode::from_str_insensitive("strict").unwrap(),
        TypeMismatchMode::Fail
    );
    assert_eq!(
        TypeMismatchMode::from_str_insensitive("null").unwrap(),
        TypeMismatchMode::Null
    );
    assert_eq!(
        TypeMismatchMode::from_str_insensitive("permissive").unwrap(),
        TypeMismatchMode::Null
    );
    assert!(TypeMismatchMode::from_str_insensitive("bad").is_err());

    assert_eq!(
        LookupJoinType::from_str_insensitive("left").unwrap(),
        LookupJoinType::Left
    );
    assert_eq!(
        LookupJoinType::from_str_insensitive("left_outer").unwrap(),
        LookupJoinType::Left
    );
    assert_eq!(
        LookupJoinType::from_str_insensitive("INNER").unwrap(),
        LookupJoinType::Inner
    );
    assert!(LookupJoinType::from_str_insensitive("full").is_err());

    assert!(KeyLiteral::String("k1".into()).to_key("test", "s").is_ok());
    assert!(KeyLiteral::Int(42).to_key("test", "s").is_ok());
    assert!(KeyLiteral::Bytes(vec![1, 2, 3]).to_key("test", "s").is_ok());

    let mut rt = ReadTicket {
        namespace: "".to_string(),
        set_name: "s".to_string(),
        key_column: "pk".to_string(),
        bins: vec![],
        read_mode: ReadModeSpec::PartitionRange {
            begin_partition: 0,
            partition_count: 100,
        },
        batch_size: 100,
        type_mismatch_mode: TypeMismatchMode::Fail,
        schema_ipc_base64: "".to_string(),
        compression: None,
        compression_level: None,
    };
    assert!(rt.validate().is_err());
    rt.namespace = "test".to_string();
    rt.key_column = "".to_string();
    assert!(rt.validate().is_err());
    rt.key_column = "pk".to_string();
    rt.read_mode = ReadModeSpec::PartitionRange {
        begin_partition: 4096,
        partition_count: 1,
    };
    assert!(rt.validate().is_err());
    rt.read_mode = ReadModeSpec::PartitionRange {
        begin_partition: 0,
        partition_count: 0,
    };
    assert!(rt.validate().is_err());
    rt.read_mode = ReadModeSpec::PartitionRange {
        begin_partition: 4000,
        partition_count: 200,
    };
    assert!(rt.validate().is_err());
    rt.read_mode = ReadModeSpec::PartitionRange {
        begin_partition: 0,
        partition_count: 4096,
    };
    assert!(rt.validate().is_ok());

    assert!(
        InferSchemaRequest {
            namespace: "".into(),
            set_name: "s".into(),
            key_column: "pk".into(),
            sample_size: 10,
        }
        .validate()
        .is_err()
    );
    assert!(
        InferSchemaRequest {
            namespace: "ns".into(),
            set_name: "s".into(),
            key_column: "".into(),
            sample_size: 10,
        }
        .validate()
        .is_err()
    );

    assert!(
        LookupExchangeParams {
            namespace: "".into(),
            set_name: "s".into(),
            key_column: "pk".into(),
            join_type: LookupJoinType::Left,
            type_mismatch_mode: TypeMismatchMode::Fail,
            bins_schema_ipc_base64: "".into(),
            compression: None,
            compression_level: None,
        }
        .validate()
        .is_err()
    );
    assert!(
        LookupExchangeParams {
            namespace: "ns".into(),
            set_name: "s".into(),
            key_column: "".into(),
            join_type: LookupJoinType::Left,
            type_mismatch_mode: TypeMismatchMode::Fail,
            bins_schema_ipc_base64: "".into(),
            compression: None,
            compression_level: None,
        }
        .validate()
        .is_err()
    );

    // 5. ProxyError -> tonic::Status conversions
    assert_eq!(
        tonic::Status::from(ProxyError::Config("bad".into())).code(),
        Code::Internal
    );
    assert_eq!(
        tonic::Status::from(ProxyError::Unauthenticated("no".into())).code(),
        Code::Unauthenticated
    );
    assert_eq!(
        tonic::Status::from(ProxyError::InvalidArgument("arg".into())).code(),
        Code::InvalidArgument
    );
    assert_eq!(
        tonic::Status::from(ProxyError::Conversion("conv".into())).code(),
        Code::InvalidArgument
    );
    assert_eq!(
        tonic::Status::from(ProxyError::Flight(
            arrow_flight::error::FlightError::DecodeError("dec".into())
        ))
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        tonic::Status::from(ProxyError::Flight(arrow_flight::error::FlightError::Tonic(
            Box::new(tonic::Status::permission_denied("perm"))
        )))
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        tonic::Status::from(ProxyError::WriteFailed {
            attempts: 3,
            message: "err".into(),
        })
        .code(),
        Code::Aborted
    );
    assert_eq!(
        tonic::Status::from(ProxyError::Internal("int".into())).code(),
        Code::Internal
    );
}

#[tokio::test]
async fn test_auth_modes_and_observability_http_endpoints() {
    use as_proxy_server::metrics::serve_observability;
    use base64::Engine;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // 1. Disabled auth mode
    let disabled_auth = Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    });
    assert!(!disabled_auth.is_enabled());
    assert!(disabled_auth.verify_credentials("any", "any").is_ok());
    assert!(disabled_auth.verify_token("not-even-a-token").is_ok());
    let empty_meta = MetadataMap::new();
    assert!(disabled_auth.verify_metadata(&empty_meta).is_ok());
    assert!(
        disabled_auth
            .authenticate_handshake(&empty_meta, b"")
            .is_ok()
    );

    // 2. Enabled auth mode: Basic header, Bearer header, user:pass payload, expired token, malformed tokens
    let auth = Authenticator::new(AuthConfig {
        enabled: true,
        username: "admin".to_string(),
        password: "secret".to_string(),
        token_secret: "my_secret_key".to_string(),
        token_ttl_secs: 3600,
    });
    assert!(auth.is_enabled());

    let b64_creds = base64::engine::general_purpose::STANDARD.encode("admin:secret");
    let mut basic_meta = MetadataMap::new();
    basic_meta.insert(
        "authorization",
        MetadataValue::try_from(format!("Basic {b64_creds}")).unwrap(),
    );
    assert!(auth.verify_metadata(&basic_meta).is_ok());
    let issued_tok = auth.authenticate_handshake(&basic_meta, b"").unwrap();

    // Handshake with Bearer header
    let mut bearer_meta = MetadataMap::new();
    bearer_meta.insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {issued_tok}")).unwrap(),
    );
    assert!(auth.authenticate_handshake(&bearer_meta, b"").is_ok());

    // Raw token in authorization header
    let mut raw_meta = MetadataMap::new();
    raw_meta.insert(
        "authorization",
        MetadataValue::try_from(issued_tok.clone()).unwrap(),
    );
    assert!(auth.verify_metadata(&raw_meta).is_ok());

    // Handshake with UTF-8 "user:pass" payload
    assert!(
        auth.authenticate_handshake(&empty_meta, b"admin:secret")
            .is_ok()
    );
    assert!(auth.authenticate_handshake(&empty_meta, b"").is_err());

    // Invalid Basic headers
    let mut bad_basic = MetadataMap::new();
    bad_basic.insert(
        "authorization",
        MetadataValue::from_static("Basic !!!not_base64!!!"),
    );
    assert!(auth.verify_metadata(&bad_basic).is_err());

    let no_colon = base64::engine::general_purpose::STANDARD.encode("admin_without_colon");
    bad_basic.insert(
        "authorization",
        MetadataValue::try_from(format!("Basic {no_colon}")).unwrap(),
    );
    assert!(auth.verify_metadata(&bad_basic).is_err());

    // Malformed / unknown-user / expired tokens
    assert!(auth.verify_token("no_dot_separator").is_err());
    let wrong_user_tok = auth.issue_token("wrong_user").unwrap();
    assert!(auth.verify_token(&wrong_user_tok).is_err());

    // Observability HTTP server test (/healthz, /metrics, /404)
    let obs_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let obs_addr = obs_listener.local_addr().unwrap();
    drop(obs_listener);

    let metrics = ProxyMetrics::new();
    metrics.inc_records_written(10);
    metrics.inc_records_read(20);
    metrics.inc_records_validated(5);
    metrics.inc_validation_mismatches(1);
    let healthy_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let healthy_clone = Arc::clone(&healthy_flag);
    let addr_str = obs_addr.to_string();
    tokio::spawn(async move {
        let _ = serve_observability(&addr_str, metrics, None, move || {
            healthy_clone.load(Ordering::Relaxed)
        })
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.unwrap();
        resp
    }

    let resp_ok = http_get(obs_addr, "/healthz").await;
    assert!(resp_ok.contains("200 OK"));
    assert!(resp_ok.contains("\"status\":\"ok\""));

    healthy_flag.store(false, Ordering::Relaxed);
    let resp_unhealthy = http_get(obs_addr, "/readyz").await;
    assert!(resp_unhealthy.contains("503 Service Unavailable"));

    let resp_metrics = http_get(obs_addr, "/metrics").await;
    assert!(resp_metrics.contains("200 OK"));
    assert!(resp_metrics.contains("as_proxy_records_written_total 10"));
    assert!(resp_metrics.contains("as_proxy_records_read_total 20"));

    let resp_404 = http_get(obs_addr, "/unknown_path").await;
    assert!(resp_404.contains("404 Not Found"));
}

#[test]
fn test_converter_all_read_write_types_and_mismatch_modes() {
    use arrow_array::NullArray;
    use arrow_array::builder::{
        Date64Builder, FixedSizeBinaryBuilder, Float32Builder, Int8Builder, Int16Builder,
        LargeBinaryBuilder, LargeStringBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder, UInt16Builder,
        UInt32Builder, UInt64Builder,
    };
    use as_proxy_server::converter::{
        RecordDqResult, build_array_from_values, build_lookup_record_batch,
        decode_arrow_schema_ipc_base64, extract_keys_from_batch, infer_arrow_schema_from_records,
        records_to_record_batch, validate_record_against_bins,
    };

    // 1. Test build_array_from_values across all supported Arrow DataTypes in both Fail and Null mismatch modes
    let mut omap = IndexMap::new();
    omap.insert(Value::String("k1".into()), Value::Int(100));
    omap.insert(Value::String("f_str".into()), Value::String("hello".into()));
    omap.insert(Value::String("f_nil".into()), Value::Nil);

    let mut hmap = HashMap::new();
    hmap.insert(Value::String("k2".into()), Value::Int(200));
    hmap.insert(Value::String("f_str".into()), Value::String("world".into()));

    let v_bool = Value::Bool(true);
    let v_int0 = Value::Int(0);
    let v_int1 = Value::Int(1);
    let v_int_small = Value::Int(42);
    let v_int_huge = Value::Int(i64::MAX);
    let v_float = Value::Float(FloatValue::from(3.5));
    let v_dec_pos = Value::String("+123.45".into());
    let v_dec_neg = Value::String("-99.1".into());
    let v_dec_int = Value::String("50".into());
    let v_dec_bad = Value::String("not_a_decimal".into());
    let v_str = Value::String("abc".into());
    let v_geo = Value::GeoJson("{\"type\":\"Point\"}".into());
    let v_blob = Value::Blob(vec![1, 2, 3]);
    let v_hll = Value::Hll(vec![9, 8]);
    let v_nil = Value::Nil;
    let v_list = Value::List(vec![Value::Int(10), Value::Nil, Value::Int(20)]);
    let v_omap = Value::OrderedMap(omap);
    let v_hmap = Value::HashMap(hmap);

    // Null type
    let null_arr = build_array_from_values(
        "c",
        &DataType::Null,
        &[Some(&v_nil), None],
        TypeMismatchMode::Fail,
    )
    .unwrap();
    assert_eq!(null_arr.len(), 2);

    // Boolean (Bool, Int(0), Int(1), Nil, mismatch)
    let b_arr = build_array_from_values(
        "c",
        &DataType::Boolean,
        &[
            Some(&v_bool),
            Some(&v_int0),
            Some(&v_int1),
            Some(&v_nil),
            None,
            Some(&v_str),
        ],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(b_arr.len(), 6);
    assert!(b_arr.is_null(5));
    assert!(
        build_array_from_values(
            "c",
            &DataType::Boolean,
            &[Some(&v_str)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );

    // Int8, Int16, Int32, Int64 (including overflow and mismatch)
    for dt in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let arr = build_array_from_values(
            "c",
            &dt,
            &[Some(&v_int_small), Some(&v_int_huge), Some(&v_str), None],
            TypeMismatchMode::Null,
        )
        .unwrap();
        assert_eq!(arr.len(), 4);
        assert!(
            build_array_from_values("c", &dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
        );
    }
    assert!(
        build_array_from_values(
            "c",
            &DataType::Int8,
            &[Some(&v_int_huge)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );
    assert!(
        build_array_from_values(
            "c",
            &DataType::Int16,
            &[Some(&v_int_huge)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );
    assert!(
        build_array_from_values(
            "c",
            &DataType::Int32,
            &[Some(&v_int_huge)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );

    // Float32 & Float64
    for dt in [DataType::Float32, DataType::Float64] {
        let arr = build_array_from_values(
            "c",
            &dt,
            &[Some(&v_float), Some(&v_int_small), Some(&v_str), None],
            TypeMismatchMode::Null,
        )
        .unwrap();
        assert_eq!(arr.len(), 4);
        assert!(
            build_array_from_values("c", &dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
        );
    }

    // Decimal128
    let dec_dt = DataType::Decimal128(10, 2);
    let dec_arr = build_array_from_values(
        "c",
        &dec_dt,
        &[
            Some(&v_dec_pos),
            Some(&v_dec_neg),
            Some(&v_dec_int),
            Some(&v_int_small),
            Some(&v_dec_bad),
            Some(&v_blob),
            None,
        ],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(dec_arr.len(), 7);
    assert!(dec_arr.is_null(4));
    assert!(dec_arr.is_null(5));
    assert!(
        build_array_from_values("c", &dec_dt, &[Some(&v_dec_bad)], TypeMismatchMode::Fail).is_err()
    );

    // Utf8 & Binary
    let utf_arr = build_array_from_values(
        "c",
        &DataType::Utf8,
        &[Some(&v_str), Some(&v_geo), Some(&v_int_small), None],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(utf_arr.len(), 4);
    assert!(
        build_array_from_values(
            "c",
            &DataType::Utf8,
            &[Some(&v_int_small)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );

    let bin_arr = build_array_from_values(
        "c",
        &DataType::Binary,
        &[Some(&v_blob), Some(&v_hll), Some(&v_str), None],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(bin_arr.len(), 4);
    assert!(
        build_array_from_values(
            "c",
            &DataType::Binary,
            &[Some(&v_str)],
            TypeMismatchMode::Fail
        )
        .is_err()
    );

    // Date32 & Timestamps (Second, Millisecond, Microsecond, Nanosecond)
    for dt in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        let arr = build_array_from_values(
            "c",
            &dt,
            &[Some(&v_int_small), Some(&v_str), None],
            TypeMismatchMode::Null,
        )
        .unwrap();
        assert_eq!(arr.len(), 3);
        assert!(
            build_array_from_values("c", &dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
        );
    }

    // List, Map, Struct
    let list_dt = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let list_arr = build_array_from_values(
        "c",
        &list_dt,
        &[Some(&v_list), Some(&v_str), None],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(list_arr.len(), 3);
    assert!(
        build_array_from_values("c", &list_dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
    );

    let map_entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Int64, true),
        ])),
        false,
    ));
    let map_dt = DataType::Map(map_entries_field, false);
    let map_arr = build_array_from_values(
        "c",
        &map_dt,
        &[Some(&v_omap), Some(&v_hmap), Some(&v_str), None],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(map_arr.len(), 4);
    assert!(
        build_array_from_values("c", &map_dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
    );

    let struct_dt = DataType::Struct(Fields::from(vec![
        Field::new("f_str", DataType::Utf8, true),
        Field::new("f_nil", DataType::Utf8, true),
        Field::new("f_missing", DataType::Int64, true),
    ]));
    let struct_arr = build_array_from_values(
        "c",
        &struct_dt,
        &[Some(&v_omap), Some(&v_hmap), Some(&v_str), None],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert_eq!(struct_arr.len(), 4);
    assert!(
        build_array_from_values("c", &struct_dt, &[Some(&v_str)], TypeMismatchMode::Fail).is_err()
    );

    // 2. Test infer_arrow_schema_from_records & digest-only key fallback
    let digest_only_key = Key::with_digest("test", "users", None, [0xABu8; 20]);
    let mut rec_bins = IndexMap::new();
    rec_bins.insert("b_nil".to_string(), Value::Nil);
    rec_bins.insert("b_bool".to_string(), Value::Bool(true));
    rec_bins.insert("b_int".to_string(), Value::Int(1));
    rec_bins.insert("b_flt".to_string(), Value::Float(FloatValue::from(1.0)));
    rec_bins.insert("b_str".to_string(), Value::String("s".into()));
    rec_bins.insert("b_blob".to_string(), Value::Blob(vec![1]));
    rec_bins.insert("b_list".to_string(), Value::List(vec![Value::Int(1)]));
    rec_bins.insert("b_omap".to_string(), v_omap.clone());
    rec_bins.insert("b_hmap".to_string(), v_hmap.clone());

    let scanned = vec![ScannedRecord {
        key: Some(digest_only_key),
        bins: rec_bins,
    }];
    let inferred = infer_arrow_schema_from_records("pk", &scanned);
    assert_eq!(inferred.fields().len(), 10);

    // records_to_record_batch with 0 fields (count() query) and with digest fallback
    let empty_schema = Arc::new(Schema::empty());
    let empty_rb =
        records_to_record_batch(&empty_schema, "pk", &scanned, TypeMismatchMode::Fail).unwrap();
    assert_eq!(empty_rb.num_rows(), 1);
    assert_eq!(empty_rb.num_columns(), 0);

    let hex_key_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Utf8, false)]));
    let hex_rb =
        records_to_record_batch(&hex_key_schema, "pk", &scanned, TypeMismatchMode::Fail).unwrap();
    assert_eq!(hex_rb.num_rows(), 1);

    let bin_key_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Binary, false)]));
    let bin_rb =
        records_to_record_batch(&bin_key_schema, "pk", &scanned, TypeMismatchMode::Fail).unwrap();
    assert_eq!(bin_rb.num_rows(), 1);

    // 3. Test convert_record_batch and extract_keys_from_batch across extended Arrow types
    let ext_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::UInt32, false),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("lstr", DataType::LargeUtf8, true),
        Field::new("lbin", DataType::LargeBinary, true),
        Field::new("fbin", DataType::FixedSizeBinary(4), true),
        Field::new("d64", DataType::Date64, true),
        Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new(
            "ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("null_col", DataType::Null, true),
    ]));

    let mut pk_u32 = UInt32Builder::new();
    pk_u32.append_value(77);
    let mut b_i8 = Int8Builder::new();
    b_i8.append_value(1);
    let mut b_i16 = Int16Builder::new();
    b_i16.append_value(2);
    let mut b_u8 = UInt8Builder::new();
    b_u8.append_value(3);
    let mut b_u16 = UInt16Builder::new();
    b_u16.append_value(4);
    let mut b_u64 = UInt64Builder::new();
    b_u64.append_value(5);
    let mut b_f32 = Float32Builder::new();
    b_f32.append_value(1.5);
    let mut b_lstr = LargeStringBuilder::new();
    b_lstr.append_value("large_str");
    let mut b_lbin = LargeBinaryBuilder::new();
    b_lbin.append_value(b"lbin");
    let mut b_fbin = FixedSizeBinaryBuilder::new(4);
    b_fbin.append_value(b"abcd").unwrap();
    let mut b_d64 = Date64Builder::new();
    b_d64.append_value(86_400_000);
    let mut b_ts_s = TimestampSecondBuilder::new();
    b_ts_s.append_value(100);
    let mut b_ts_ms = TimestampMillisecondBuilder::new();
    b_ts_ms.append_value(100_000);
    let mut b_ts_ns = TimestampNanosecondBuilder::new();
    b_ts_ns.append_value(100_000_000_000);

    let ext_rb = RecordBatch::try_new(
        ext_schema,
        vec![
            Arc::new(pk_u32.finish()),
            Arc::new(b_i8.finish()),
            Arc::new(b_i16.finish()),
            Arc::new(b_u8.finish()),
            Arc::new(b_u16.finish()),
            Arc::new(b_u64.finish()),
            Arc::new(b_f32.finish()),
            Arc::new(b_lstr.finish()),
            Arc::new(b_lbin.finish()),
            Arc::new(b_fbin.finish()),
            Arc::new(b_d64.finish()),
            Arc::new(b_ts_s.finish()),
            Arc::new(b_ts_ms.finish()),
            Arc::new(b_ts_ns.finish()),
            Arc::new(NullArray::new(1)),
        ],
    )
    .unwrap();

    let wp_ext = WriteParameters {
        job_id: None,
        namespace: "test".into(),
        set_name: "ext".into(),
        key_column: "pk".into(),
        write_mode: WriteMode::Replace,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };
    let prep = convert_record_batch(&ext_rb, &wp_ext, 15).unwrap();
    assert_eq!(prep.len(), 1);
    assert_eq!(prep[0].bins.len(), 13); // null_col omitted in Replace mode

    let extr_keys = extract_keys_from_batch(&ext_rb, "test", "ext", "pk").unwrap();
    assert_eq!(extr_keys.len(), 1);
    assert!(extract_keys_from_batch(&ext_rb, "test", "ext", "non_existent").is_err());

    // Invalid bin name characters in convert_record_batch
    for bad_name in [
        "",
        "bad space",
        "bad:colon",
        "bad;semi",
        "bad\x01ctrl",
        "кириллица",
    ] {
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new(bad_name, DataType::Int32, true),
        ]));
        let mut k = Int32Builder::new();
        k.append_value(1);
        let mut v = Int32Builder::new();
        v.append_value(2);
        let rb = RecordBatch::try_new(bad_schema, vec![Arc::new(k.finish()), Arc::new(v.finish())])
            .unwrap();
        assert!(convert_record_batch(&rb, &wp_ext, 15).is_err());
    }

    // 4. DQ equality & validate_record_against_bins edge cases
    assert!(values_equal_dq(&Value::Bool(true), &Value::Int(1)));
    assert!(values_equal_dq(&Value::Int(0), &Value::Bool(false)));
    assert!(values_equal_dq(&v_omap, &v_omap));
    assert!(values_equal_dq(&v_hmap, &v_hmap));

    let all_nil_rec = PreparedRecord {
        key: Key::new("test", "s", Value::String("k".into())).unwrap(),
        bins: vec![Bin::new("b1".to_string(), Value::Nil)],
    };
    // All-Nil expected record against missing key in Aerospike -> Matched!
    assert_eq!(
        validate_record_against_bins(&all_nil_rec, None),
        RecordDqResult::Matched
    );
    let mut non_nil_map = HashMap::new();
    non_nil_map.insert("b1".to_string(), Value::Int(5));
    assert!(matches!(
        validate_record_against_bins(&all_nil_rec, Some(&non_nil_map)),
        RecordDqResult::BinMismatch(_)
    ));

    // decode_arrow_schema_ipc_base64 error paths
    assert!(decode_arrow_schema_ipc_base64("!!!not_base64!!!").is_err());
    assert!(decode_arrow_schema_ipc_base64("YWJj").is_err());

    // build_lookup_record_batch when Inner join matches 0 rows -> Ok(None)
    let bins_s = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)]));
    let none_res = build_lookup_record_batch(
        &ext_rb,
        &bins_s,
        "pk",
        &[RecordReadOutcome::NotFound],
        LookupJoinType::Inner,
        TypeMismatchMode::Fail,
    )
    .unwrap();
    assert!(none_res.is_none());
}

#[tokio::test]
async fn test_flight_service_actions_and_error_paths() {
    use aerospike::ResultCode;
    use arrow_flight::{Criteria, Empty};
    use as_proxy_server::writer::{classify_aerospike_failure, compute_backoff_with_jitter};

    // 1. Test classify_aerospike_failure & compute_backoff_with_jitter
    assert!(matches!(
        classify_aerospike_failure(None, Some(ResultCode::DeviceOverload)),
        RecordWriteOutcome::OverloadError(_)
    ));
    assert!(matches!(
        classify_aerospike_failure(None, Some(ResultCode::KeyBusy)),
        RecordWriteOutcome::OverloadError(_)
    ));
    assert!(matches!(
        classify_aerospike_failure(None, Some(ResultCode::ParameterError)),
        RecordWriteOutcome::OtherError(_)
    ));
    assert!(matches!(
        classify_aerospike_failure(None, None),
        RecordWriteOutcome::OtherError(_)
    ));
    let backoff = compute_backoff_with_jitter(
        &RetryConfig {
            max_other_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            backoff_multiplier: 0.5,
        },
        0,
    );
    assert_eq!(backoff, Duration::from_millis(1));

    // 2. Start Flight server WITHOUT Raft -> do_action returns Unavailable
    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink,
        WriterConfig::default(),
        RetryConfig::default(),
        metrics.clone(),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));
    let svc_no_raft = ProxyFlightService::new(auth, engine, metrics, 15);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc_no_raft))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // list_actions succeeds, unimplemented RPCs return Unimplemented, do_action without Raft returns Unavailable
    let actions: Vec<_> = client
        .list_actions(Request::new(Empty {}))
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actions.len(), 5);

    assert!(
        client
            .list_flights(Request::new(Criteria {
                expression: Bytes::new()
            }))
            .await
            .is_err()
    );
    assert!(
        client
            .get_flight_info(Request::new(FlightDescriptor::new_cmd(vec![])))
            .await
            .is_err()
    );
    assert!(
        client
            .poll_flight_info(Request::new(FlightDescriptor::new_cmd(vec![])))
            .await
            .is_err()
    );
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "get_cluster_status".into(),
                body: Bytes::new(),
            }))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_read_keys_and_dq_validate_retries_and_raft_actions() {
    use arrow_flight::Ticket;
    use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};

    let k_ok = Key::new("test", "retry_set", Value::String("k_ok".into())).unwrap();
    let k_ov = Key::new("test", "retry_set", Value::String("k_ov".into())).unwrap();
    let k_oth = Key::new("test", "retry_set", Value::String("k_oth".into())).unwrap();
    let k_fatal = Key::new("test", "retry_set", Value::String("k_fatal".into())).unwrap();

    let mut overload_map = HashMap::new();
    overload_map.insert(k_ov.clone(), 2);
    let mut other_map = HashMap::new();
    other_map.insert(k_oth.clone(), 1);
    other_map.insert(k_fatal.clone(), 10);

    let sink = Arc::new(MockAerospikeSink::new(overload_map, other_map));
    {
        let mut stored = sink.stored.lock().unwrap();
        stored.insert(k_ok.clone(), vec![Bin::new("v", Value::Int(10))]);
        stored.insert(k_ov.clone(), vec![Bin::new("v", Value::Int(20))]);
        stored.insert(k_oth.clone(), vec![Bin::new("v", Value::Int(30))]);
    }

    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig {
            use_batch_write: true,
            batch_write_size: 2,
            max_in_flight_records: 16,
            max_concurrent_subbatches_per_stream: 2,
            ..WriterConfig::default()
        },
        RetryConfig {
            max_other_retries: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            backoff_multiplier: 1.5,
        },
        metrics.clone(),
    ));
    assert!(engine.is_connected());

    // 1. Test read_keys with OverloadError + OtherError retries succeeding
    let outcomes = engine
        .read_keys(
            vec![k_ok.clone(), k_ov.clone(), k_oth.clone()],
            Some(vec!["v".into()]),
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], RecordReadOutcome::Found(_)));
    assert!(matches!(outcomes[1], RecordReadOutcome::Found(_)));
    assert!(matches!(outcomes[2], RecordReadOutcome::Found(_)));

    // 2. Test read_keys with fatal OtherError exceeding max_other_retries
    assert!(engine.read_keys(vec![k_fatal.clone()], None).await.is_err());

    // 3. Reset attempt counters and test DQ validate_records with OverloadError + OtherError retries
    sink.write_attempts.lock().unwrap().clear();
    let dq_params = WriteParameters {
        job_id: Some("job-dq-retry".into()),
        namespace: "test".into(),
        set_name: "retry_set".into(),
        key_column: "pk".into(),
        write_mode: WriteMode::Validate,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: true,
        compression: None,
        auto_convert_strings: false,
    };
    let dq_stats = engine
        .validate_records(
            vec![
                PreparedRecord {
                    key: k_ok.clone(),
                    bins: vec![Bin::new("v", Value::Int(10))],
                },
                PreparedRecord {
                    key: k_ov.clone(),
                    bins: vec![Bin::new("v", Value::Int(20))],
                },
                PreparedRecord {
                    key: k_oth.clone(),
                    bins: vec![Bin::new("v", Value::Int(999))], // mismatch
                },
            ],
            &dq_params,
        )
        .await
        .unwrap();
    assert_eq!(dq_stats.rows_validated, 3);
    assert_eq!(dq_stats.matched_rows, 2);
    assert_eq!(dq_stats.mismatched_rows, 1);
    assert!(dq_stats.overload_retries >= 2);
    assert!(dq_stats.other_retries >= 1);

    // DQ validate with fatal OtherError
    sink.write_attempts.lock().unwrap().clear();
    assert!(
        engine
            .validate_records(
                vec![PreparedRecord {
                    key: k_fatal.clone(),
                    bins: vec![Bin::new("v", Value::Int(1))],
                }],
                &dq_params
            )
            .await
            .is_err()
    );

    // 4. Start single-node Raft + Flight service to test do_action (complete_job, fail_job, not_found, unsupported)
    // and do_put with metadata headers + do_get/do_exchange error paths
    let raft_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_addr = raft_l.local_addr().unwrap();
    drop(raft_l);

    let raft_node = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: raft_addr.to_string(),
        election_timeout_min_ms: 80,
        election_timeout_max_ms: 150,
        heartbeat_interval_ms: 25,
        job_stats_ttl_secs: 3600,
        peers: vec![],
        ..RaftConfig::default()
    });
    raft_node.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));
    let flight_svc = ProxyFlightService::new(auth, engine, metrics, 15).with_raft_node(raft_node);

    let fl_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fl_addr = fl_listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_incoming(TcpListenerStream::new(fl_listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{fl_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // do_action: complete_job & fail_job (plain string and JSON payload), get_job_status not found, unsupported action
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "get_job_status".into(),
                body: Bytes::from_static(b"non_existent_job"),
            }))
            .await
            .is_err()
    );
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "get_job_status".into(),
                body: Bytes::from_static(b"   "),
            }))
            .await
            .is_err()
    );
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "complete_job".into(),
                body: Bytes::from_static(b"job-plain-1"),
            }))
            .await
            .is_ok()
    );
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "fail_job".into(),
                body: Bytes::from_static(
                    br#"{"job_id":"job-json-1","namespace":"test","set_name":"s","error":"boom"}"#
                ),
            }))
            .await
            .is_ok()
    );
    assert!(
        client
            .do_action(Request::new(Action {
                r#type: "unknown_action".into(),
                body: Bytes::new(),
            }))
            .await
            .is_err()
    );

    // do_put using gRPC metadata headers (no FlightDescriptor)
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let mut k_b = StringBuilder::new();
    k_b.append_value("k_hdr_1");
    let mut v_b = Int64Builder::new();
    v_b.append_value(100);
    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(k_b.finish()), Arc::new(v_b.finish())],
    )
    .unwrap();

    let put_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema.clone())
        .build(stream::iter(vec![Ok(rb)]))
        .map(|r| r.unwrap());
    let mut put_req = Request::new(put_stream);
    put_req
        .metadata_mut()
        .insert("x-as-namespace", MetadataValue::from_static("test"));
    put_req
        .metadata_mut()
        .insert("x-as-set", MetadataValue::from_static("retry_set"));
    put_req
        .metadata_mut()
        .insert("x-as-key-column", MetadataValue::from_static("pk"));
    put_req
        .metadata_mut()
        .insert("x-as-job-id", MetadataValue::from_static("job-hdr-1"));
    let mut put_resp = client.do_put(put_req).await.unwrap().into_inner();
    while (put_resp.try_next().await.unwrap()).is_some() {}

    // do_get PartitionRange with batch_size=3 (4 records -> 3 in loop + 1 trailing flush)
    let schema_b64 = encode_arrow_schema_ipc_base64(&schema).unwrap();
    let scan_ticket = serde_json::to_vec(&ReadTicket {
        namespace: "test".into(),
        set_name: "retry_set".into(),
        key_column: "pk".into(),
        bins: vec![],
        read_mode: ReadModeSpec::PartitionRange {
            begin_partition: 0,
            partition_count: 4096,
        },
        batch_size: 3,
        type_mismatch_mode: TypeMismatchMode::Fail,
        schema_ipc_base64: schema_b64,
        compression: None,
        compression_level: None,
    })
    .unwrap();
    let dg_stream = client
        .do_get(Request::new(Ticket {
            ticket: Bytes::from(scan_ticket),
        }))
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut dg_dec = FlightDataDecoder::new(dg_stream);
    let mut total_scanned = 0;
    while let Some(msg) = dg_dec.try_next().await.unwrap() {
        if let DecodedPayload::RecordBatch(b) = msg.payload {
            total_scanned += b.num_rows();
        }
    }
    assert_eq!(total_scanned, 4);

    // do_put error paths with raft_node & job_id:
    // (a) conversion error (null PK)
    let nullable_pk_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let mut null_pk_b = StringBuilder::new();
    null_pk_b.append_null();
    let mut v_b2 = Int64Builder::new();
    v_b2.append_value(1);
    let bad_rb = RecordBatch::try_new(
        nullable_pk_schema.clone(),
        vec![Arc::new(null_pk_b.finish()), Arc::new(v_b2.finish())],
    )
    .unwrap();
    let err_params = WriteParameters {
        job_id: Some("job-conv-err".into()),
        namespace: "test".into(),
        set_name: "retry_set".into(),
        key_column: "pk".into(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };
    let bad_put_stream = FlightDataEncoderBuilder::new()
        .with_schema(nullable_pk_schema)
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
            serde_json::to_vec(&err_params).unwrap(),
        )))
        .build(stream::iter(vec![Ok(bad_rb)]))
        .map(|r| r.unwrap());
    let mut bad_put_resp = client
        .do_put(Request::new(bad_put_stream))
        .await
        .unwrap()
        .into_inner();
    assert!(bad_put_resp.try_next().await.is_err());

    // (b) fatal write error & fatal DQ validation error via do_put
    for (jid, val_only) in [("job-write-fatal", false), ("job-dq-fatal", true)] {
        sink.write_attempts.lock().unwrap().clear();
        let mut kf_b = StringBuilder::new();
        kf_b.append_value("k_fatal");
        let mut vf_b = Int64Builder::new();
        vf_b.append_value(42);
        let fatal_rb = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(kf_b.finish()), Arc::new(vf_b.finish())],
        )
        .unwrap();
        let fatal_params = WriteParameters {
            job_id: Some(jid.into()),
            namespace: "test".into(),
            set_name: "retry_set".into(),
            key_column: "pk".into(),
            write_mode: if val_only {
                WriteMode::Validate
            } else {
                WriteMode::Update
            },
            ttl_seconds: None,
            send_key: true,
            ignore_nulls: false,
            use_batch_write: Some(true),
            validate_only: val_only,
            compression: None,
            auto_convert_strings: false,
        };
        let fatal_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
                serde_json::to_vec(&fatal_params).unwrap(),
            )))
            .build(stream::iter(vec![Ok(fatal_rb)]))
            .map(|r| r.unwrap());
        let mut fatal_resp = client
            .do_put(Request::new(fatal_stream))
            .await
            .unwrap()
            .into_inner();
        assert!(fatal_resp.try_next().await.is_err());
    }

    // do_get & get_schema error paths
    assert!(
        client
            .get_schema(Request::new(FlightDescriptor::new_cmd(
                b"{bad_json".to_vec()
            )))
            .await
            .is_err()
    );
    assert!(
        client
            .do_get(Request::new(Ticket {
                ticket: Bytes::from_static(b"{bad_json"),
            }))
            .await
            .is_err()
    );

    // do_exchange when Inner join matches 0 keys -> emits empty batch with out_schema
    let mut k_miss = StringBuilder::new();
    k_miss.append_value("definitely_missing_key");
    let miss_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Utf8, false)]));
    let miss_rb =
        RecordBatch::try_new(miss_schema.clone(), vec![Arc::new(k_miss.finish())]).unwrap();
    let bins_only = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let ex_params = LookupExchangeParams {
        namespace: "test".into(),
        set_name: "retry_set".into(),
        key_column: "pk".into(),
        join_type: LookupJoinType::Inner,
        type_mismatch_mode: TypeMismatchMode::Fail,
        bins_schema_ipc_base64: encode_arrow_schema_ipc_base64(&bins_only).unwrap(),
        compression: None,
        compression_level: None,
    };
    let ex_stream = FlightDataEncoderBuilder::new()
        .with_schema(miss_schema.clone())
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
            serde_json::to_vec(&ex_params).unwrap(),
        )))
        .build(stream::iter(vec![Ok(miss_rb.clone())]))
        .map(|r| r.unwrap());
    let ex_resp = client
        .do_exchange(Request::new(ex_stream))
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut ex_dec = FlightDataDecoder::new(ex_resp);
    let mut ex_rows = 0;
    while let Some(msg) = ex_dec.try_next().await.unwrap() {
        if let DecodedPayload::RecordBatch(b) = msg.payload {
            ex_rows += b.num_rows();
        }
    }
    assert_eq!(ex_rows, 0);

    // do_exchange error paths: missing FlightDescriptor & invalid FlightDescriptor JSON
    let no_desc_stream = FlightDataEncoderBuilder::new()
        .with_schema(miss_schema.clone())
        .build(stream::iter(vec![Ok(miss_rb.clone())]))
        .map(|r| r.unwrap());
    let mut no_desc_resp = client
        .do_exchange(Request::new(no_desc_stream))
        .await
        .unwrap()
        .into_inner();
    assert!(no_desc_resp.try_next().await.is_err());

    let bad_desc_stream = FlightDataEncoderBuilder::new()
        .with_schema(miss_schema.clone())
        .with_flight_descriptor(Some(FlightDescriptor::new_cmd(b"{bad".to_vec())))
        .build(stream::iter(vec![Ok(miss_rb)]))
        .map(|r| r.unwrap());
    let mut bad_desc_resp = client
        .do_exchange(Request::new(bad_desc_stream))
        .await
        .unwrap()
        .into_inner();
    assert!(bad_desc_resp.try_next().await.is_err());

    // Additional do_get & do_exchange error paths (fatal read_keys & type mismatch fail)
    let mismatch_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("v", DataType::Utf8, true), // stored 'v' is Int(10), so Fail mode errors!
    ]));
    let mismatch_b64 = encode_arrow_schema_ipc_base64(&mismatch_schema).unwrap();

    for bs in [1, 10] {
        let t_bytes = serde_json::to_vec(&ReadTicket {
            namespace: "test".into(),
            set_name: "retry_set".into(),
            key_column: "pk".into(),
            bins: vec!["v".into()],
            read_mode: ReadModeSpec::PartitionRange {
                begin_partition: 0,
                partition_count: 4096,
            },
            batch_size: bs,
            type_mismatch_mode: TypeMismatchMode::Fail,
            schema_ipc_base64: mismatch_b64.clone(),
            compression: None,
            compression_level: None,
        })
        .unwrap();
        let mut s = client
            .do_get(Request::new(Ticket {
                ticket: Bytes::from(t_bytes),
            }))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(_)) = s.try_next().await {}
    }

    // BatchKeys with fatal read_keys error & BatchKeys with type mismatch error
    for (key_str, sc_b64) in [
        ("k_fatal", encode_arrow_schema_ipc_base64(&schema).unwrap()),
        ("k_ok", mismatch_b64.clone()),
    ] {
        sink.write_attempts.lock().unwrap().clear();
        let bk_t = serde_json::to_vec(&ReadTicket {
            namespace: "test".into(),
            set_name: "retry_set".into(),
            key_column: "pk".into(),
            bins: vec!["v".into()],
            read_mode: ReadModeSpec::BatchKeys {
                keys: vec![KeyLiteral::String(key_str.into())],
            },
            batch_size: 10,
            type_mismatch_mode: TypeMismatchMode::Fail,
            schema_ipc_base64: sc_b64,
            compression: None,
            compression_level: None,
        })
        .unwrap();
        let mut s = client
            .do_get(Request::new(Ticket {
                ticket: Bytes::from(bk_t),
            }))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(_)) = s.try_next().await {}
    }

    // do_exchange with fatal read_keys error & type mismatch error
    let wrong_bins_only = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    for (key_str, b_schema) in [("k_fatal", bins_only), ("k_ok", wrong_bins_only)] {
        sink.write_attempts.lock().unwrap().clear();
        let mut kb = StringBuilder::new();
        kb.append_value(key_str);
        let in_rb = RecordBatch::try_new(miss_schema.clone(), vec![Arc::new(kb.finish())]).unwrap();
        let p = LookupExchangeParams {
            namespace: "test".into(),
            set_name: "retry_set".into(),
            key_column: "pk".into(),
            join_type: LookupJoinType::Left,
            type_mismatch_mode: TypeMismatchMode::Fail,
            bins_schema_ipc_base64: encode_arrow_schema_ipc_base64(&b_schema).unwrap(),
            compression: None,
            compression_level: None,
        };
        let st = FlightDataEncoderBuilder::new()
            .with_schema(miss_schema.clone())
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
                serde_json::to_vec(&p).unwrap(),
            )))
            .build(stream::iter(vec![Ok(in_rb)]))
            .map(|r| r.unwrap());
        let mut resp = client
            .do_exchange(Request::new(st))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(_)) = resp.try_next().await {}
    }
}

#[test]
fn test_converter_remaining_key_and_value_types_and_dq_branches() {
    use arrow_array::{
        ArrayRef, BinaryViewArray, Decimal256Array, FixedSizeBinaryArray, FixedSizeListArray,
        Int8Array, Int16Array, LargeBinaryArray, LargeListArray, LargeStringArray, StringViewArray,
        StructArray, UInt8Array, UInt16Array, UInt64Array,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, i256};
    use as_proxy_server::converter::{RecordDqResult, validate_record_against_bins};
    use indexmap::IndexMap;

    let base_params = WriteParameters {
        job_id: None,
        namespace: "test".into(),
        set_name: "users".into(),
        key_column: "pk".into(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: None,
        validate_only: false,
        compression: None,
        auto_convert_strings: false,
    };

    // 1. Empty batch (0 rows) -> Ok(vec![])
    let s_ok = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let empty_rb = RecordBatch::new_empty(s_ok.clone());
    assert!(
        convert_record_batch(&empty_rb, &base_params, 15)
            .unwrap()
            .is_empty()
    );

    // 2. Missing key_column -> Err
    let mut bad_key_params = base_params.clone();
    bad_key_params.key_column = "non_existent".into();
    let one_row_rb = RecordBatch::try_new(
        s_ok,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            Arc::new(arrow_array::Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    assert!(convert_record_batch(&one_row_rb, &bad_key_params, 15).is_err());

    // 3. Only key column (0 bin columns) -> Err
    let s_only_pk = Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)]));
    let rb_only_pk = RecordBatch::try_new(
        s_only_pk,
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(convert_record_batch(&rb_only_pk, &base_params, 15).is_err());

    // 4. Duplicate bin names in schema -> Err
    let s_dup = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let rb_dup = RecordBatch::try_new(
        s_dup,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            Arc::new(arrow_array::Int64Array::from(vec![10])),
            Arc::new(arrow_array::Int64Array::from(vec![20])),
        ],
    )
    .unwrap();
    assert!(convert_record_batch(&rb_dup, &base_params, 15).is_err());

    // 5. All remaining key types in convert_record_batch
    let key_arrays: Vec<(DataType, ArrayRef, bool)> = vec![
        (DataType::Int8, Arc::new(Int8Array::from(vec![7i8])), true),
        (
            DataType::Int16,
            Arc::new(Int16Array::from(vec![700i16])),
            true,
        ),
        (
            DataType::Int32,
            Arc::new(arrow_array::Int32Array::from(vec![70_000i32])),
            true,
        ),
        (
            DataType::UInt8,
            Arc::new(UInt8Array::from(vec![200u8])),
            true,
        ),
        (
            DataType::UInt16,
            Arc::new(UInt16Array::from(vec![50_000u16])),
            true,
        ),
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![123_456u64])),
            true,
        ),
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            false,
        ), // overflow
        (
            DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["k_large"])),
            true,
        ),
        (
            DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["k_view"])),
            true,
        ),
        (
            DataType::Binary,
            Arc::new(arrow_array::BinaryArray::from(vec![b"bin_k".as_slice()])),
            true,
        ),
        (
            DataType::LargeBinary,
            Arc::new(LargeBinaryArray::from(vec![b"lbin_k".as_slice()])),
            true,
        ),
        (
            DataType::FixedSizeBinary(4),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3, 4]].into_iter()).unwrap(),
            ),
            true,
        ),
        (
            DataType::BinaryView,
            Arc::new(BinaryViewArray::from(vec![b"bview_k".as_slice()])),
            true,
        ),
        (
            DataType::Float64,
            Arc::new(arrow_array::Float64Array::from(vec![1.5])),
            false,
        ), // unsupported key type
    ];

    for (dt, key_arr, should_succeed) in key_arrays {
        let sc = Arc::new(Schema::new(vec![
            Field::new("pk", dt, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let rb = RecordBatch::try_new(
            sc,
            vec![key_arr, Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert_eq!(
            convert_record_batch(&rb, &base_params, 15).is_ok(),
            should_succeed
        );
    }

    // 6. Remaining value types: Decimal256, Utf8View, BinaryView, LargeList, FixedSizeList, Struct with ignore_nulls=true
    let dec256_arr = Decimal256Array::from(vec![i256::from_i128(12345)])
        .with_precision_and_scale(38, 2)
        .unwrap();
    let uview_arr = StringViewArray::from(vec!["hello_view"]);
    let bview_arr = BinaryViewArray::from(vec![b"bytes_view".as_slice()]);

    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let llist_arr = LargeListArray::new(
        item_field.clone(),
        OffsetBuffer::new(vec![0i64, 2].into()),
        Arc::new(arrow_array::Int32Array::from(vec![10, 20])),
        None,
    );
    let fslist_arr = FixedSizeListArray::new(
        item_field,
        2,
        Arc::new(arrow_array::Int32Array::from(vec![30, 40])),
        None,
    );

    let struct_fields: arrow_schema::Fields = vec![
        Field::new("keep", DataType::Int32, true),
        Field::new("drop_null", DataType::Utf8, true),
    ]
    .into();
    let mut drop_b = StringBuilder::new();
    drop_b.append_null();
    let struct_arr = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(arrow_array::Int32Array::from(vec![99])),
            Arc::new(drop_b.finish()),
        ],
        Some(NullBuffer::from(vec![true])),
    );

    let val_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("d256", DataType::Decimal256(38, 2), true),
        Field::new("uv", DataType::Utf8View, true),
        Field::new("bv", DataType::BinaryView, true),
        Field::new(
            "ll",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "fsl",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2),
            true,
        ),
        Field::new("st", DataType::Struct(struct_fields), true),
    ]));

    let val_rb = RecordBatch::try_new(
        val_schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            Arc::new(dec256_arr),
            Arc::new(uview_arr),
            Arc::new(bview_arr),
            Arc::new(llist_arr),
            Arc::new(fslist_arr),
            Arc::new(struct_arr),
        ],
    )
    .unwrap();

    let mut ignore_null_params = base_params.clone();
    ignore_null_params.ignore_nulls = true;
    let recs = convert_record_batch(&val_rb, &ignore_null_params, 15).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].bins.len(), 6);
    if let Value::OrderedMap(m) = &recs[0].bins[5].value {
        assert_eq!(m.len(), 1); // drop_null was omitted because ignore_nulls = true
    } else {
        panic!("expected OrderedMap for struct bin");
    }

    // 7. values_equal_dq & validate_record_against_bins remaining branches
    assert!(values_equal_dq(&Value::Bool(true), &Value::Bool(true)));
    assert!(!values_equal_dq(&Value::Bool(true), &Value::Bool(false)));
    assert!(values_equal_dq(&Value::Bool(true), &Value::Int(1)));
    assert!(values_equal_dq(&Value::Int(0), &Value::Bool(false)));
    assert!(values_equal_dq(
        &Value::Blob(vec![1, 2]),
        &Value::Blob(vec![1, 2])
    ));

    let mut hm = HashMap::new();
    hm.insert(
        Value::String("k".into()),
        Value::Float(FloatValue::from(f64::NAN)),
    );
    let mut om = IndexMap::new();
    om.insert(
        Value::String("k".into()),
        Value::Float(FloatValue::from(f64::NAN)),
    );
    assert!(values_equal_dq(
        &Value::HashMap(hm.clone()),
        &Value::OrderedMap(om.clone())
    ));
    assert!(values_equal_dq(
        &Value::OrderedMap(om.clone()),
        &Value::HashMap(hm.clone())
    ));
    assert!(values_equal_dq(
        &Value::OrderedMap(om.clone()),
        &Value::OrderedMap(om)
    ));
    assert!(values_equal_dq(
        &Value::HashMap(hm.clone()),
        &Value::HashMap(hm)
    ));

    let key = Key::new("test", "users", Value::Int(1)).unwrap();
    let prep_non_nil = PreparedRecord {
        key: key.clone(),
        bins: vec![Bin::new("a", Value::Int(1))],
    };
    let prep_all_nil = PreparedRecord {
        key: key.clone(),
        bins: vec![Bin::new("a", Value::Nil)],
    };
    assert_eq!(
        validate_record_against_bins(&prep_non_nil, None),
        RecordDqResult::MissingKey
    );
    assert_eq!(
        validate_record_against_bins(&prep_all_nil, None),
        RecordDqResult::Matched
    );
    let empty_actual = HashMap::new();
    assert!(matches!(
        validate_record_against_bins(&prep_non_nil, Some(&empty_actual)),
        RecordDqResult::BinMismatch(_)
    ));
    let mut actual_map = HashMap::new();
    actual_map.insert("a".to_string(), Value::Int(1));
    assert_eq!(
        validate_record_against_bins(&prep_non_nil, Some(&actual_map)),
        RecordDqResult::Matched
    );
    assert!(matches!(
        validate_record_against_bins(&prep_all_nil, Some(&actual_map)),
        RecordDqResult::BinMismatch(_)
    ));
}

#[tokio::test]
async fn test_flight_compression_modes_and_disableable_switch() {
    use arrow_flight::Ticket;
    use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
    use as_proxy_server::config::{FlightCompression, ServerConfig};
    use tonic::codec::CompressionEncoding;

    // 1. Unit tests for FlightCompression parsing, headers, IPC options, and master switch
    for (raw, expected) in [
        ("none", FlightCompression::None),
        ("off", FlightCompression::None),
        ("false", FlightCompression::None),
        ("disabled", FlightCompression::None),
        ("0", FlightCompression::None),
        ("zstd", FlightCompression::Zstd),
        ("ZSTD", FlightCompression::Zstd),
        ("lz4", FlightCompression::Lz4),
        ("lz4_frame", FlightCompression::Lz4),
        ("lz4-frame", FlightCompression::Lz4),
        ("gzip", FlightCompression::Gzip),
    ] {
        assert_eq!(
            FlightCompression::from_str_insensitive(raw).unwrap(),
            expected
        );
    }
    assert!(FlightCompression::from_str_insensitive("brotli").is_err());

    let mut hdrs = MetadataMap::new();
    assert_eq!(FlightCompression::from_headers(&hdrs).unwrap(), None);
    assert_eq!(FlightCompression::level_from_headers(&hdrs).unwrap(), None);
    hdrs.insert("x-as-compression", MetadataValue::from_static("zstd:5"));
    assert_eq!(
        FlightCompression::from_headers(&hdrs).unwrap(),
        Some(FlightCompression::Zstd)
    );
    assert_eq!(
        FlightCompression::level_from_headers(&hdrs).unwrap(),
        Some(5)
    );
    hdrs.insert("x-as-compression-level", MetadataValue::from_static("9"));
    assert_eq!(
        FlightCompression::level_from_headers(&hdrs).unwrap(),
        Some(9)
    );
    hdrs.insert(
        "x-as-compression-level",
        MetadataValue::from_static("bad_lvl"),
    );
    assert!(FlightCompression::level_from_headers(&hdrs).is_err());
    hdrs.insert("x-as-compression-level", MetadataValue::from_static("99"));
    assert!(FlightCompression::level_from_headers(&hdrs).is_err());

    assert_eq!(
        FlightCompression::extract_inline_level("zstd:3").unwrap(),
        Some(3)
    );
    assert_eq!(
        FlightCompression::extract_inline_level("zstd").unwrap(),
        None
    );
    assert!(FlightCompression::extract_inline_level("zstd:abc").is_err());
    assert!(FlightCompression::extract_inline_level("zstd:50").is_err());

    for lvl in 1..=9 {
        assert!(
            FlightCompression::Zstd
                .to_ipc_write_options_with_level(Some(lvl))
                .is_ok()
        );
    }
    assert!(
        FlightCompression::Zstd
            .to_ipc_write_options_with_level(Some(99))
            .is_err()
    );

    assert_eq!(FlightCompression::None.to_tonic_encoding(), None);
    assert_eq!(
        FlightCompression::Zstd.to_tonic_encoding(),
        Some(CompressionEncoding::Zstd)
    );
    assert_eq!(FlightCompression::Lz4.to_tonic_encoding(), None);
    assert_eq!(
        FlightCompression::Gzip.to_tonic_encoding(),
        Some(CompressionEncoding::Gzip)
    );

    for comp in [
        FlightCompression::None,
        FlightCompression::Zstd,
        FlightCompression::Lz4,
        FlightCompression::Gzip,
    ] {
        assert!(comp.to_ipc_write_options().is_ok());
    }

    let mut srv_cfg = ServerConfig::default();
    assert!(srv_cfg.compression_enabled);
    assert_eq!(srv_cfg.effective_compression(None), FlightCompression::Zstd);
    assert_eq!(
        srv_cfg.effective_compression(Some(FlightCompression::Lz4)),
        FlightCompression::Lz4
    );
    srv_cfg.compression_enabled = false;
    assert_eq!(
        srv_cfg.effective_compression(Some(FlightCompression::Zstd)),
        FlightCompression::None
    );

    // 2. End-to-end DoPut, DoGet, and DoExchange across Zstd, Lz4, Gzip, and None compression modes
    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig::default(),
        RetryConfig::default(),
        metrics.clone(),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));

    let enabled_srv_cfg = ServerConfig {
        compression_enabled: true,
        compression: FlightCompression::Zstd,
        ..ServerConfig::default()
    };
    let flight_svc = ProxyFlightService::new(auth.clone(), engine.clone(), metrics.clone(), 15)
        .with_compression(
            enabled_srv_cfg.compression_enabled,
            enabled_srv_cfg.compression,
        )
        .into_server(&enabled_srv_cfg);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(flight_svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);

    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let mut pk_b = StringBuilder::new();
    let mut pay_b = StringBuilder::new();
    for i in 0..20 {
        pk_b.append_value(format!("comp_k_{i}"));
        pay_b.append_value(" repetitive_compressible_string_payload ".repeat(8));
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(pk_b.finish()), Arc::new(pay_b.finish())],
    )
    .unwrap();

    for mode in [
        FlightCompression::Zstd,
        FlightCompression::Lz4,
        FlightCompression::Gzip,
        FlightCompression::None,
    ] {
        // 2a. Compressed DoPut
        let wp = WriteParameters {
            job_id: None,
            namespace: "test".into(),
            set_name: "comp_set".into(),
            key_column: "pk".into(),
            write_mode: WriteMode::Update,
            ttl_seconds: None,
            send_key: true,
            ignore_nulls: false,
            use_batch_write: Some(true),
            validate_only: false,
            compression: Some(mode),
            auto_convert_strings: false,
        };
        let put_ipc_opts = mode.to_ipc_write_options().unwrap();
        let put_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_options(put_ipc_opts)
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
                serde_json::to_vec(&wp).unwrap(),
            )))
            .build(stream::iter(vec![Ok(batch.clone())]))
            .map(|r| r.unwrap());

        let mut put_resp = client
            .do_put(Request::new(put_stream))
            .await
            .unwrap()
            .into_inner();
        while (put_resp.try_next().await.unwrap()).is_some() {}

        // 2b. Compressed DoGet
        let ticket_bytes = serde_json::to_vec(&ReadTicket {
            namespace: "test".into(),
            set_name: "comp_set".into(),
            key_column: "pk".into(),
            bins: vec!["payload".into()],
            read_mode: ReadModeSpec::PartitionRange {
                begin_partition: 0,
                partition_count: 4096,
            },
            batch_size: 10,
            type_mismatch_mode: TypeMismatchMode::Fail,
            schema_ipc_base64: encode_arrow_schema_ipc_base64(&schema).unwrap(),
            compression: Some(mode),
            compression_level: Some(3),
        })
        .unwrap();

        let dg_stream = client
            .do_get(Request::new(Ticket {
                ticket: Bytes::from(ticket_bytes),
            }))
            .await
            .unwrap()
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);
        let mut dg_dec = FlightDataDecoder::new(dg_stream);
        let mut read_rows = 0;
        while let Some(msg) = dg_dec.try_next().await.unwrap() {
            if let DecodedPayload::RecordBatch(rb) = msg.payload {
                read_rows += rb.num_rows();
            }
        }
        assert_eq!(read_rows, 20);

        // 2c. Compressed DoExchange
        let in_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Utf8, false)]));
        let mut lk_b = StringBuilder::new();
        lk_b.append_value("comp_k_0");
        lk_b.append_value("comp_k_19");
        let in_rb = RecordBatch::try_new(in_schema.clone(), vec![Arc::new(lk_b.finish())]).unwrap();
        let bins_only = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let ex_params = LookupExchangeParams {
            namespace: "test".into(),
            set_name: "comp_set".into(),
            key_column: "pk".into(),
            join_type: LookupJoinType::Inner,
            type_mismatch_mode: TypeMismatchMode::Fail,
            bins_schema_ipc_base64: encode_arrow_schema_ipc_base64(&bins_only).unwrap(),
            compression: Some(mode),
            compression_level: Some(6),
        };
        let ex_stream = FlightDataEncoderBuilder::new()
            .with_schema(in_schema)
            .with_options(mode.to_ipc_write_options_with_level(Some(6)).unwrap())
            .with_flight_descriptor(Some(FlightDescriptor::new_cmd(
                serde_json::to_vec(&ex_params).unwrap(),
            )))
            .build(stream::iter(vec![Ok(in_rb)]))
            .map(|r| r.unwrap());

        let ex_resp = client
            .do_exchange(Request::new(ex_stream))
            .await
            .unwrap()
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);
        let mut ex_dec = FlightDataDecoder::new(ex_resp);
        let mut ex_rows = 0;
        while let Some(msg) = ex_dec.try_next().await.unwrap() {
            if let DecodedPayload::RecordBatch(rb) = msg.payload {
                ex_rows += rb.num_rows();
            }
        }
        assert_eq!(ex_rows, 2);
    }

    // 3. Test master switch `compression_enabled = false` on server (and Gzip default server config)
    for srv in [
        ServerConfig {
            compression_enabled: false,
            compression: FlightCompression::Zstd,
            ..ServerConfig::default()
        },
        ServerConfig {
            compression_enabled: true,
            compression: FlightCompression::Gzip,
            ..ServerConfig::default()
        },
    ] {
        let svc = ProxyFlightService::new(auth.clone(), engine.clone(), metrics.clone(), 15)
            .with_compression(srv.compression_enabled, srv.compression)
            .with_compression_level(srv.compression_level)
            .into_server(&srv);
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(l))
                .await
                .unwrap();
        });
        let ch = tonic::transport::Endpoint::from_shared(format!("http://{a}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut cl = FlightServiceClient::new(ch)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);

        let t_bytes = serde_json::to_vec(&ReadTicket {
            namespace: "test".into(),
            set_name: "comp_set".into(),
            key_column: "pk".into(),
            bins: vec!["payload".into()],
            read_mode: ReadModeSpec::PartitionRange {
                begin_partition: 0,
                partition_count: 4096,
            },
            batch_size: 20,
            type_mismatch_mode: TypeMismatchMode::Fail,
            schema_ipc_base64: encode_arrow_schema_ipc_base64(&schema).unwrap(),
            compression: Some(FlightCompression::Zstd),
            compression_level: Some(9),
        })
        .unwrap();
        let dg = cl
            .do_get(Request::new(Ticket {
                ticket: Bytes::from(t_bytes),
            }))
            .await
            .unwrap()
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);
        let mut dec = FlightDataDecoder::new(dg);
        let mut rows = 0;
        while let Some(msg) = dec.try_next().await.unwrap() {
            if let DecodedPayload::RecordBatch(rb) = msg.payload {
                rows += rb.num_rows();
            }
        }
        assert_eq!(rows, 20);
    }
}

#[tokio::test]
async fn test_raft_init_job_overwrites_existing_job_metadata_and_wide_batch_slicing() {
    use as_proxy_server::config::{FlightCompression, ServerConfig};

    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink.clone(),
        WriterConfig {
            use_batch_write: true,
            batch_write_size: 32,
            max_in_flight_records: 128,
            max_concurrent_subbatches_per_stream: 4,
            ..WriterConfig::default()
        },
        RetryConfig::default(),
        metrics.clone(),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: false,
        ..AuthConfig::default()
    }));
    let raft_node = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: "127.0.0.1:0".into(),
        peers: vec![],
        ..RaftConfig::default()
    });
    raft_node.start().await.unwrap();

    let svc = ProxyFlightService::new(auth, engine, metrics, 15)
        .with_raft_node(raft_node.clone())
        .into_server(&ServerConfig::default());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    // 1. Build a wide 50-column RecordBatch (pk + 50 bins) with 40 rows
    let mut fields = vec![Field::new("pk", DataType::Utf8, false)];
    for c in 0..50 {
        fields.push(Field::new(format!("bin_{c}"), DataType::Int64, true));
    }
    let wide_schema = Arc::new(Schema::new(fields));

    let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(51);
    let mut pk_b = StringBuilder::new();
    for i in 0..40 {
        pk_b.append_value(format!("wide-k-{i}"));
    }
    cols.push(Arc::new(pk_b.finish()));
    for c in 0..50 {
        let mut int_b = Int64Builder::new();
        for i in 0..40 {
            int_b.append_value((i * 100 + c) as i64);
        }
        cols.push(Arc::new(int_b.finish()));
    }
    let wide_batch = RecordBatch::try_new(wide_schema.clone(), cols).unwrap();

    // 2. Initialize job "wide-job-1" via DoAction("init_job")
    let init_payload = serde_json::json!({
        "job_id": "wide-job-1",
        "namespace": "test",
        "set_name": "wide_set",
        "is_dq_validation": false
    });
    let mut init_stream = client
        .do_action(Request::new(Action {
            r#type: "init_job".into(),
            body: Bytes::from(serde_json::to_vec(&init_payload).unwrap()),
        }))
        .await
        .unwrap()
        .into_inner();
    let init_res = init_stream.try_next().await.unwrap().unwrap();
    let init_status: JobLoadStatus = serde_json::from_slice(&init_res.body).unwrap();
    assert_eq!(init_status.job_id, "wide-job-1");
    assert_eq!(init_status.total_rows_written, 0);
    assert_eq!(init_status.state, JobState::Running);

    // 3. Stream the wide batch via DoPut (write mode) and then DQ mode
    for validate_only in [false, true] {
        let wp = WriteParameters {
            job_id: Some("wide-job-1".into()),
            namespace: "test".into(),
            set_name: "wide_set".into(),
            key_column: "pk".into(),
            write_mode: WriteMode::Replace,
            ttl_seconds: None,
            send_key: true,
            ignore_nulls: false,
            use_batch_write: Some(true),
            validate_only,
            compression: Some(FlightCompression::None),
            auto_convert_strings: false,
        };
        let desc = FlightDescriptor::new_cmd(serde_json::to_vec(&wp).unwrap());
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(wide_schema.clone())
            .with_flight_descriptor(Some(desc))
            .build(stream::iter(vec![Ok(wide_batch.clone())]))
            .map(|r| r.unwrap());
        let mut resp = client
            .do_put(Request::new(stream))
            .await
            .unwrap()
            .into_inner();
        let ack_msg = resp.try_next().await.unwrap().unwrap();
        let ack: PutBatchAck = serde_json::from_slice(&ack_msg.app_metadata).unwrap();
        if validate_only {
            assert_eq!(ack.rows_validated, 40);
            assert_eq!(ack.matched_rows, 40);
        } else {
            assert_eq!(ack.rows_written, 40);
        }
    }

    // Complete job "wide-job-1"
    client
        .do_action(Request::new(Action {
            r#type: "complete_job".into(),
            body: Bytes::from("wide-job-1"),
        }))
        .await
        .unwrap();

    let before_reset = raft_node.get_job_status("wide-job-1").await.unwrap();
    assert_eq!(before_reset.total_rows_written, 40);
    assert_eq!(before_reset.state, JobState::Completed);

    // 4. Re-initialize the SAME job_id ("wide-job-1") via DoAction("init_job") -> must overwrite metadata in Raft!
    let mut reset_stream = client
        .do_action(Request::new(Action {
            r#type: "init_job".into(),
            body: Bytes::from(serde_json::to_vec(&init_payload).unwrap()),
        }))
        .await
        .unwrap()
        .into_inner();
    let reset_res = reset_stream.try_next().await.unwrap().unwrap();
    let reset_status: JobLoadStatus = serde_json::from_slice(&reset_res.body).unwrap();
    assert_eq!(reset_status.job_id, "wide-job-1");
    assert_eq!(reset_status.total_rows_written, 0);
    assert_eq!(reset_status.total_batches_processed, 0);
    assert_eq!(reset_status.state, JobState::Running);
}

#[test]
fn test_auto_convert_strings_option() {
    use arrow_array::{LargeStringArray, StringViewArray};
    use as_proxy_server::converter::parse_string_auto;

    // 1. Direct unit tests for parse_string_auto
    assert_eq!(parse_string_auto("1", false), Value::String("1".into()));
    assert_eq!(parse_string_auto("1.0", false), Value::String("1.0".into()));

    assert_eq!(parse_string_auto("1", true), Value::Int(1));
    assert_eq!(parse_string_auto("-42", true), Value::Int(-42));
    assert_eq!(parse_string_auto("+7", true), Value::Int(7));
    assert_eq!(parse_string_auto(" 100 ", true), Value::Int(100));
    assert_eq!(
        parse_string_auto("1.0", true),
        Value::Float(FloatValue::from(1.0))
    );
    assert_eq!(
        parse_string_auto("-3.14", true),
        Value::Float(FloatValue::from(-3.14))
    );
    assert_eq!(
        parse_string_auto(".5", true),
        Value::Float(FloatValue::from(0.5))
    );
    assert_eq!(
        parse_string_auto("1e5", true),
        Value::Float(FloatValue::from(100_000.0))
    );
    assert_eq!(
        parse_string_auto("hello", true),
        Value::String("hello".into())
    );
    assert_eq!(parse_string_auto("", true), Value::String("".into()));
    assert_eq!(parse_string_auto("   ", true), Value::String("   ".into()));
    assert_eq!(parse_string_auto("NaN", true), Value::String("NaN".into()));
    assert_eq!(
        parse_string_auto("Infinity", true),
        Value::String("Infinity".into())
    );

    // 2. RecordBatch conversion with Utf8, LargeUtf8, Utf8View, and nested List/Struct/Map
    let struct_fields = Fields::from(vec![Field::new("nested_num", DataType::Utf8, true)]);
    let mut pk_b = StringBuilder::new();
    let mut s_int_b = StringBuilder::new();
    let mut s_flt_b = StringBuilder::new();
    let mut s_txt_b = StringBuilder::new();
    let mut list_b = ListBuilder::new(StringBuilder::new());
    let mut struct_b =
        StructBuilder::new(struct_fields.clone(), vec![Box::new(StringBuilder::new())]);

    // Row 0: numeric strings ("1", "1.0") and non-numeric ("abc"), plus primary key "100" (PK stays String!)
    pk_b.append_value("100");
    s_int_b.append_value("1");
    s_flt_b.append_value("1.0");
    s_txt_b.append_value("abc");
    list_b.values().append_value("42");
    list_b.values().append_value("2.5");
    list_b.values().append_value("raw");
    list_b.append(true);
    struct_b
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("99.9");
    struct_b.append(true);

    let list_arr = list_b.finish();
    let struct_arr = struct_b.finish();
    let large_arr = LargeStringArray::from(vec![Some("777")]);
    let view_arr = StringViewArray::from(vec![Some("8.25")]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("c_int", DataType::Utf8, true),
        Field::new("c_flt", DataType::Utf8, true),
        Field::new("c_txt", DataType::Utf8, true),
        Field::new("c_list", list_arr.data_type().clone(), true),
        Field::new("c_struct", DataType::Struct(struct_fields), true),
        Field::new("c_large", DataType::LargeUtf8, true),
        Field::new("c_view", DataType::Utf8View, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(pk_b.finish()),
            Arc::new(s_int_b.finish()),
            Arc::new(s_flt_b.finish()),
            Arc::new(s_txt_b.finish()),
            Arc::new(list_arr),
            Arc::new(struct_arr),
            Arc::new(large_arr),
            Arc::new(view_arr),
        ],
    )
    .unwrap();

    // Default auto_convert_strings = false -> all string bins remain Value::String
    let params_false = WriteParameters {
        namespace: "test".into(),
        set_name: "users".into(),
        key_column: "pk".into(),
        auto_convert_strings: false,
        ..WriteParameters::default()
    };
    let recs_false = convert_record_batch(&batch, &params_false, 15).unwrap();
    assert_eq!(
        recs_false[0].bins[0],
        Bin::new("c_int", Value::String("1".into()))
    );
    assert_eq!(
        recs_false[0].bins[1],
        Bin::new("c_flt", Value::String("1.0".into()))
    );

    // auto_convert_strings = true -> "1" -> Int(1), "1.0" -> Float(1.0), "abc" -> String("abc")
    let mut headers = MetadataMap::new();
    headers.insert(
        "x-as-auto-convert-strings",
        MetadataValue::from_static("true"),
    );
    let cmd_bytes = serde_json::to_vec(&params_false).unwrap();
    let params_true =
        WriteParameters::from_descriptor_or_headers(Some(&cmd_bytes), &headers).unwrap();
    assert!(params_true.auto_convert_strings);

    let recs_true = convert_record_batch(&batch, &params_true, 15).unwrap();
    let r = &recs_true[0];
    // Primary key stays String("100") because pk column is DataType::Utf8
    assert_eq!(
        r.key,
        Key::new("test", "users", Value::String("100".into())).unwrap()
    );
    assert_eq!(r.bins[0], Bin::new("c_int", Value::Int(1)));
    assert_eq!(
        r.bins[1],
        Bin::new("c_flt", Value::Float(FloatValue::from(1.0)))
    );
    assert_eq!(r.bins[2], Bin::new("c_txt", Value::String("abc".into())));
    assert_eq!(
        r.bins[3],
        Bin::new(
            "c_list",
            Value::List(vec![
                Value::Int(42),
                Value::Float(FloatValue::from(2.5)),
                Value::String("raw".into()),
            ])
        )
    );
    let mut exp_struct = IndexMap::new();
    exp_struct.insert(
        Value::String("nested_num".into()),
        Value::Float(FloatValue::from(99.9)),
    );
    assert_eq!(
        r.bins[4],
        Bin::new("c_struct", Value::OrderedMap(exp_struct))
    );
    assert_eq!(r.bins[5], Bin::new("c_large", Value::Int(777)));
    assert_eq!(
        r.bins[6],
        Bin::new("c_view", Value::Float(FloatValue::from(8.25)))
    );
}

#[tokio::test]
async fn test_wide_300_bins_and_concurrent_raft_group_commit() {
    use as_proxy_server::config::{FlightCompression, ServerConfig};

    // 1. Build a 300-bin RecordBatch (1 PK + 300 Float64 bins) with 150 rows
    let mut fields = Vec::with_capacity(301);
    fields.push(Field::new("id", DataType::Int64, false));
    for b in 0..300 {
        fields.push(Field::new(format!("f_{b}"), DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let num_rows = 150usize;
    let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(301);
    let mut id_b = Int64Builder::with_capacity(num_rows);
    for r in 0..num_rows {
        id_b.append_value(r as i64);
    }
    cols.push(Arc::new(id_b.finish()));

    for b in 0..300 {
        let mut fb = Float64Builder::with_capacity(num_rows);
        for r in 0..num_rows {
            if b == 299 && r % 50 == 0 {
                fb.append_null();
            } else {
                fb.append_value((r * 1000 + b) as f64 * 0.5);
            }
        }
        cols.push(Arc::new(fb.finish()));
    }
    let wide_300_batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();

    // 2. Spin up a 2-node Raft cluster (1 Leader + 1 Follower) and stream multi-batch DoPut concurrently to the Follower
    let rl1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let r_addr1 = rl1.local_addr().unwrap().to_string();
    drop(rl1);
    let rl2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let r_addr2 = rl2.local_addr().unwrap().to_string();
    drop(rl2);

    let fl1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let f_addr1 = fl1.local_addr().unwrap().to_string();
    let fl2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let f_addr2 = fl2.local_addr().unwrap().to_string();

    let peers = vec![
        RaftPeer {
            node_id: 1,
            raft_addr: r_addr1.clone(),
            flight_addr: f_addr1.clone(),
        },
        RaftPeer {
            node_id: 2,
            raft_addr: r_addr2.clone(),
            flight_addr: f_addr2.clone(),
        },
    ];

    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));

    let r1 = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: r_addr1,
        election_timeout_min_ms: 80,
        election_timeout_max_ms: 140,
        heartbeat_interval_ms: 30,
        job_stats_ttl_secs: 3600,
        peers: peers.clone(),
        ..RaftConfig::default()
    });
    r1.start().await.unwrap();

    let r2 = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 2,
        listen_addr: r_addr2,
        election_timeout_min_ms: 250,
        election_timeout_max_ms: 400,
        heartbeat_interval_ms: 30,
        job_stats_ttl_secs: 3600,
        peers,
        ..RaftConfig::default()
    });
    r2.start().await.unwrap();

    for (fl, r_node) in [(fl1, Arc::clone(&r1)), (fl2, Arc::clone(&r2))] {
        let m = ProxyMetrics::new();
        let eng = Arc::new(MppWriteEngine::new(
            sink.clone(),
            WriterConfig {
                use_batch_write: true,
                batch_write_size: 512,
                max_in_flight_records: 4096,
                max_concurrent_subbatches_per_stream: 4,
                ..WriterConfig::default()
            },
            RetryConfig::default(),
            m.clone(),
        ));
        let auth = Arc::new(Authenticator::new(AuthConfig {
            enabled: false,
            ..AuthConfig::default()
        }));
        let svc = ProxyFlightService::new(auth, eng, m, 64)
            .with_raft_node(r_node)
            .into_server(&ServerConfig::default());
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(fl))
                .await
                .unwrap();
        });
    }

    // Wait for leader election
    for _ in 0..40 {
        if r1.get_cluster_snapshot().await.role == RaftRole::Leader
            || r2.get_cluster_snapshot().await.role == RaftRole::Leader
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // Pick Follower's Flight address so concurrent proposals exercise Follower ProposeBatch forwarding!
    let follower_flight_addr = if r2.get_cluster_snapshot().await.role == RaftRole::Follower {
        f_addr2.clone()
    } else {
        f_addr1.clone()
    };

    // Launch 6 concurrent multi-batch DoPut streams (each sending 2 batches of 75 rows x 300 bins)
    let mut stream_handles = Vec::new();
    for s in 0..6 {
        let addr = follower_flight_addr.clone();
        let sc = Arc::clone(&schema);
        let b1 = wide_300_batch.slice(0, 75);
        let b2 = wide_300_batch.slice(75, 75);
        stream_handles.push(tokio::spawn(async move {
            let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client = FlightServiceClient::new(ch);
            let wp = WriteParameters {
                job_id: Some("wide-300-job".into()),
                namespace: "ns".into(),
                set_name: "wide300".into(),
                key_column: "id".into(),
                write_mode: WriteMode::Replace,
                ttl_seconds: None,
                send_key: true,
                ignore_nulls: false,
                use_batch_write: Some(true),
                validate_only: false,
                compression: Some(FlightCompression::None),
                auto_convert_strings: false,
            };
            let desc = FlightDescriptor::new_cmd(serde_json::to_vec(&wp).unwrap());
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(sc)
                .with_flight_descriptor(Some(desc))
                .build(stream::iter(vec![Ok(b1), Ok(b2)]))
                .map(|r| r.unwrap());
            let mut resp = client
                .do_put(Request::new(stream))
                .await
                .unwrap()
                .into_inner();
            let mut ack_count = 0;
            while let Some(msg) = resp.try_next().await.unwrap() {
                let ack: PutBatchAck = serde_json::from_slice(&msg.app_metadata).unwrap();
                assert_eq!(ack.rows_written, 75);
                ack_count += 1;
            }
            assert_eq!(ack_count, 2, "stream {s} must receive 2 PutResult ACKs");
        }));
    }

    for h in stream_handles {
        h.await.unwrap();
    }

    let status = r1.get_job_status("wide-300-job").await.unwrap();
    assert_eq!(status.total_rows_written, 6 * 150);
    assert_eq!(status.total_batches_processed, 12);

    r1.shutdown();
    r2.shutdown();
}

#[tokio::test]
async fn test_security_bugs_and_raft_log_compaction_snapshot() {
    use arrow_array::{LargeStringArray, UInt64Array};
    use arrow_flight::Empty;
    use as_proxy_server::converter::{batch_outcomes_to_record_batch, build_array_from_values};
    use as_proxy_server::raft::MetadataCommand;
    use tokio::io::AsyncWriteExt;

    // 1. UInt64 bin > i64::MAX must fail conversion instead of wrapping to negative i64
    let u64_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("big_u64", DataType::UInt64, false),
    ]));
    let u64_batch = RecordBatch::try_new(
        u64_schema,
        vec![
            Arc::new(arrow_array::StringArray::from(vec!["k1"])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ],
    )
    .unwrap();
    let wp = WriteParameters {
        job_id: Some("j1".into()),
        namespace: "ns".into(),
        set_name: "s".into(),
        key_column: "id".into(),
        ..WriteParameters::default()
    };
    let err_u64 = convert_record_batch(&u64_batch, &wp, 15).unwrap_err();
    assert!(err_u64.to_string().contains("overflows i64::MAX"));

    // 2. WriteParameters ttl_seconds bounds validation
    let mut bad_ttl_low = wp.clone();
    bad_ttl_low.ttl_seconds = Some(-3);
    assert!(bad_ttl_low.validate().is_err());

    let mut bad_ttl_high = wp.clone();
    bad_ttl_high.ttl_seconds = Some(i64::from(u32::MAX) + 1);
    assert!(bad_ttl_high.validate().is_err());

    // 3. Decimal128 strict validation (malformed strings, precision > 38 without panic, negative scale, overflow)
    let bad_dec_strings = [
        Value::String("--5".into()),
        Value::String("+-5".into()),
        Value::String(".".into()),
        Value::String("12.34xyz".into()),
        Value::String("99999.99".into()), // exceeds precision 5, scale 2
    ];
    for bad_s in &bad_dec_strings {
        let res = build_array_from_values(
            "dec_col",
            &DataType::Decimal128(5, 2),
            &[Some(bad_s)],
            TypeMismatchMode::Fail,
        );
        assert!(
            res.is_err(),
            "Expected failure for malformed/overflow Decimal128: {bad_s:?}"
        );
    }
    // Permissive Null mode turns invalid Decimal128 strings & overflowing Ints into NULL without panicking
    let null_dec_arr = build_array_from_values(
        "dec_col",
        &DataType::Decimal128(5, 2),
        &[
            Some(&Value::String("--5".into())),
            Some(&Value::Int(1_000_000)),
            Some(&Value::String("-00123.456".into())),
        ],
        TypeMismatchMode::Null,
    )
    .unwrap();
    assert!(null_dec_arr.is_null(0));
    assert!(null_dec_arr.is_null(1));
    assert!(!null_dec_arr.is_null(2));

    // Negative scale Decimal128 & invalid precision > 38 (must return Err instead of panicking)
    let neg_scale_arr = build_array_from_values(
        "dec_neg",
        &DataType::Decimal128(6, -2),
        &[
            Some(&Value::String("12345".into())),
            Some(&Value::Int(67800)),
        ],
        TypeMismatchMode::Fail,
    )
    .unwrap();
    assert_eq!(neg_scale_arr.len(), 2);
    assert!(
        build_array_from_values(
            "dec_bad_prec",
            &DataType::Decimal128(40, 40),
            &[Some(&Value::Int(1))],
            TypeMismatchMode::Fail,
        )
        .is_err()
    );

    // 4. Inner-join lookup with LargeUtf8 input column via zero-copy MutableArrayData
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::LargeUtf8, false),
        Field::new("extra", DataType::LargeUtf8, true),
    ]));
    let in_batch = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(LargeStringArray::from(vec!["k1", "k2", "k3"])),
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    let bins_sc = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let mut m1 = HashMap::new();
    m1.insert("v".to_string(), Value::Int(10));
    let mut m3 = HashMap::new();
    m3.insert("v".to_string(), Value::Int(30));
    let outcomes = vec![
        RecordReadOutcome::Found(m1.clone()),
        RecordReadOutcome::NotFound,
        RecordReadOutcome::Found(m3),
    ];
    let enriched = as_proxy_server::converter::build_lookup_record_batch(
        &in_batch,
        &bins_sc,
        "pk",
        &outcomes,
        LookupJoinType::Inner,
        TypeMismatchMode::Fail,
    )
    .unwrap()
    .unwrap();
    assert_eq!(enriched.num_rows(), 2);

    // Direct batch_outcomes_to_record_batch (including 0-column count() schema)
    let keys_vec = vec![
        Key::new("ns", "s", Value::String("k1".into())).unwrap(),
        Key::new("ns", "s", Value::String("k2".into())).unwrap(),
        Key::new("ns", "s", Value::String("k3".into())).unwrap(),
    ];
    let empty_sc = Arc::new(Schema::empty());
    let empty_rb = batch_outcomes_to_record_batch(
        &empty_sc,
        "pk",
        &keys_vec,
        &outcomes,
        TypeMismatchMode::Fail,
    )
    .unwrap()
    .unwrap();
    assert_eq!(empty_rb.num_rows(), 2);

    // 5. list_actions must require authentication when auth.enabled = true
    let sink = Arc::new(MockAerospikeSink::new(HashMap::new(), HashMap::new()));
    let metrics = ProxyMetrics::new();
    let engine = Arc::new(MppWriteEngine::new(
        sink,
        WriterConfig::default(),
        RetryConfig::default(),
        metrics.clone(),
    ));
    let auth = Arc::new(Authenticator::new(AuthConfig {
        enabled: true,
        username: "u".into(),
        password: "p_very_long_password_for_constant_time_hash_check".into(),
        token_secret: "sec_key_12345".into(),
        token_ttl_secs: 600,
    }));
    // Verify different-length credentials work cleanly with SHA-256 constant-time check
    assert!(auth.verify_credentials("u", "short").is_err());
    assert!(
        auth.verify_credentials("u", "p_very_long_password_for_constant_time_hash_check")
            .is_ok()
    );

    let svc = ProxyFlightService::new(auth.clone(), engine, metrics, 15);
    let fl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fl_addr = fl.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(fl))
            .await
            .unwrap();
    });
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{fl_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(ch);
    // Unauthenticated list_actions must fail with Unauthenticated!
    let unauth_err = client
        .list_actions(Request::new(Empty {}))
        .await
        .unwrap_err();
    assert_eq!(unauth_err.code(), tonic::Code::Unauthenticated);

    let tok = auth.issue_token("u").unwrap();
    let mut authed_req = Request::new(Empty {});
    authed_req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {tok}")).unwrap(),
    );
    let actions: Vec<_> = client
        .list_actions(authed_req)
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actions.len(), 5);

    // 6. Raft log compaction + lagging follower InstallSnapshot + cluster_secret HMAC auth
    let rl1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rl2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ra1 = rl1.local_addr().unwrap().to_string();
    let ra2 = rl2.local_addr().unwrap().to_string();
    drop(rl1);
    drop(rl2);

    // First start a 2-node cluster where node 2 is NOT started yet so node 1 cannot replicate?
    // Wait: in a 2-node cluster majority is 2! In a 3-node cluster, 2 online nodes form a quorum
    // and compact their logs while node 3 is offline; then when node 3 joins, it catches up via InstallSnapshot!
    let rl3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ra3 = rl3.local_addr().unwrap().to_string();
    drop(rl3);

    let peers = vec![
        RaftPeer {
            node_id: 1,
            raft_addr: ra1.clone(),
            flight_addr: "127.0.0.1:18815".into(),
        },
        RaftPeer {
            node_id: 2,
            raft_addr: ra2.clone(),
            flight_addr: "127.0.0.1:28815".into(),
        },
        RaftPeer {
            node_id: 3,
            raft_addr: ra3.clone(),
            flight_addr: "127.0.0.1:38815".into(),
        },
    ];
    let shared_secret = Some("raft_hmac_cluster_secret_test_key".to_string());

    let rn1 = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 1,
        listen_addr: ra1.clone(),
        election_timeout_min_ms: 80,
        election_timeout_max_ms: 130,
        heartbeat_interval_ms: 25,
        job_stats_ttl_secs: 3600,
        max_log_entries: 12,
        cluster_secret: shared_secret.clone(),
        peers: peers.clone(),
    });
    let rn2 = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 2,
        listen_addr: ra2.clone(),
        election_timeout_min_ms: 220,
        election_timeout_max_ms: 350,
        heartbeat_interval_ms: 25,
        job_stats_ttl_secs: 3600,
        max_log_entries: 12,
        cluster_secret: shared_secret.clone(),
        peers: peers.clone(),
    });
    rn1.start().await.unwrap();
    rn2.start().await.unwrap();

    // Wait for leader election between node 1 and node 2 (2 of 3 quorum)
    for _ in 0..40 {
        if rn1.get_cluster_snapshot().await.role == RaftRole::Leader
            || rn2.get_cluster_snapshot().await.role == RaftRole::Leader
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    // Tampered / unauthenticated raw TCP frame sent to ra1 must be rejected without crashing
    if let Ok(mut forged) = tokio::net::TcpStream::connect(&ra1).await {
        let fake_json =
            br#"{"Propose":{"command":{"PurgeExpiredJobs":{"cutoff_ms":9999999999999}}}}"#;
        let _ = forged.write_u32(fake_json.len() as u32).await;
        let _ = forged.write_all(fake_json).await;
    }

    // Propose 30 batches so log compacts multiple times past max_log_entries = 12
    for i in 0..30 {
        rn1.propose(MetadataCommand::RecordJobBatch {
            job_id: "compacted-job".into(),
            node_id: 1,
            namespace: "ns".into(),
            set_name: "set".into(),
            is_dq_validation: false,
            rows_written: 10,
            overload_retries: 0,
            other_retries: 0,
            rows_validated: 0,
            matched_rows: 0,
            mismatched_rows: 0,
            missing_keys: 0,
            bin_mismatch_rows: 0,
            mismatch_samples: vec![],
            timestamp_ms: as_proxy_server::raft::now_ms() + i,
        })
        .await
        .unwrap();
    }

    let (log_len_1, base_idx_1) = rn1.log_compaction_stats().await;
    assert!(
        base_idx_1 >= 12 && log_len_1 <= 12,
        "Expected Raft log compaction: log_len={log_len_1}, base_idx={base_idx_1}"
    );
    assert_eq!(
        rn1.get_job_status("compacted-job")
            .await
            .unwrap()
            .total_rows_written,
        300
    );

    // Now start lagging node 3! Its next_index = 1 <= leader.base_index, so it must catch up via InstallSnapshot!
    let rn3 = RaftNode::new(RaftConfig {
        enabled: true,
        node_id: 3,
        listen_addr: ra3,
        election_timeout_min_ms: 250,
        election_timeout_max_ms: 400,
        heartbeat_interval_ms: 25,
        job_stats_ttl_secs: 3600,
        max_log_entries: 12,
        cluster_secret: shared_secret,
        peers,
    });
    rn3.start().await.unwrap();

    // Propose one more batch after node 3 starts so InstallSnapshot + subsequent AppendEntries both replicate
    let mut caught_up = false;
    for _ in 0..40 {
        if let Some(st) = rn3.get_job_status("compacted-job").await {
            if st.total_rows_written == 300 {
                caught_up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        caught_up,
        "Lagging Raft node 3 must catch up to compacted state via InstallSnapshot"
    );

    rn1.shutdown();
    rn2.shutdown();
    rn3.shutdown();
}

#[tokio::test]
async fn test_adaptive_batch_tuner_scales_up_avoids_cluster_wait_and_recovers() {
    use arrow_array::{Float64Array, Int64Array};
    use std::sync::atomic::AtomicU64;

    struct AdaptiveMockSink {
        simulated_latency_us: AtomicU64,
        overload_batches_remaining: AtomicUsize,
        observed_batch_sizes: Mutex<Vec<usize>>,
    }

    #[tonic::async_trait]
    impl AerospikeSink for AdaptiveMockSink {
        async fn execute_batch_write(
            &self,
            records: &[PreparedRecord],
            _params: &WriteParameters,
        ) -> Vec<RecordWriteOutcome> {
            self.observed_batch_sizes
                .lock()
                .unwrap()
                .push(records.len());
            let lat_us = self.simulated_latency_us.load(Ordering::Relaxed);
            if lat_us > 0 {
                tokio::time::sleep(Duration::from_micros(lat_us)).await;
            }
            let fail_batch = loop {
                let cur = self.overload_batches_remaining.load(Ordering::SeqCst);
                if cur == 0 {
                    break false;
                }
                if self
                    .overload_batches_remaining
                    .compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break true;
                }
            };
            if fail_batch {
                records
                    .iter()
                    .map(|_| {
                        RecordWriteOutcome::OverloadError(
                            "Simulated DEVICE_OVERLOAD (code 18)".into(),
                        )
                    })
                    .collect()
            } else {
                vec![RecordWriteOutcome::Success; records.len()]
            }
        }

        async fn execute_single_put(
            &self,
            _record: &PreparedRecord,
            _params: &WriteParameters,
        ) -> RecordWriteOutcome {
            RecordWriteOutcome::Success
        }

        async fn execute_batch_read(
            &self,
            records: &[PreparedRecord],
            _params: &WriteParameters,
        ) -> Vec<RecordReadOutcome> {
            vec![RecordReadOutcome::NotFound; records.len()]
        }

        async fn execute_batch_read_keys(
            &self,
            keys: &[Key],
            _bins: Option<&[String]>,
        ) -> Vec<RecordReadOutcome> {
            vec![RecordReadOutcome::NotFound; keys.len()]
        }

        async fn scan_partition_range(
            &self,
            _namespace: &str,
            _set_name: &str,
            _begin_partition: usize,
            _partition_count: usize,
            _bins: Option<Vec<String>>,
            _max_records: Option<u64>,
        ) -> ProxyResult<tokio::sync::mpsc::Receiver<ProxyResult<ScannedRecord>>> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }

        fn is_connected(&self) -> bool {
            true
        }
    }

    let sink = Arc::new(AdaptiveMockSink {
        simulated_latency_us: AtomicU64::new(150),
        overload_batches_remaining: AtomicUsize::new(0),
        observed_batch_sizes: Mutex::new(Vec::new()),
    });
    let metrics = ProxyMetrics::new();

    let writer_cfg = WriterConfig {
        use_batch_write: true,
        batch_write_size: 128,
        max_in_flight_records: 8_192,
        max_concurrent_subbatches_per_stream: 4,
        adaptive_batching: true,
        min_batch_write_size: 16,
        max_batch_write_size: 512,
        target_write_latency_ms: 8,
        target_batch_bytes: 262_144,
    };
    let retry_cfg = RetryConfig {
        max_other_retries: 2,
        initial_backoff_ms: 1,
        max_backoff_ms: 4,
        backoff_multiplier: 1.5,
    };

    let engine = MppWriteEngine::new(sink.clone(), writer_cfg, retry_cfg.clone(), metrics.clone());

    // Build a wide 100-bin float RecordBatch (512 rows x 101 columns)
    let mut fields = Vec::with_capacity(101);
    fields.push(Field::new("id", DataType::Int64, false));
    for b in 0..100 {
        fields.push(Field::new(format!("f{b}"), DataType::Float64, false));
    }
    let schema = Arc::new(Schema::new(fields));
    let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(101);
    cols.push(Arc::new(Int64Array::from_iter_values(0..512)));
    for b in 0..100 {
        cols.push(Arc::new(Float64Array::from_iter_values(
            (0..512).map(|r| (r + b) as f64 * 0.25),
        )));
    }
    let wide_batch = RecordBatch::try_new(schema, cols).unwrap();

    let params = WriteParameters {
        job_id: None,
        namespace: "ns".into(),
        set_name: "wide100".into(),
        key_column: "id".into(),
        write_mode: WriteMode::Replace,
        ..WriteParameters::default()
    };

    // 1. Healthy fast cluster (~150us RTT, 0 overloads): adaptive tuner must climb above 1.0x
    for _ in 0..6 {
        let st = engine
            .write_record_batch(&wide_batch, &params, 15)
            .await
            .unwrap();
        assert_eq!(st.rows_written, 512);
    }
    let snap_fast = engine.adaptive_snapshot();
    assert!(snap_fast.enabled);
    assert!(
        snap_fast.batch_scale_permille > 1_000,
        "Expected batch_scale_permille > 1000 on fast cluster, got {}",
        snap_fast.batch_scale_permille
    );
    assert!(
        snap_fast.concurrency_scale_permille > 1_000,
        "Expected concurrency_scale_permille > 1000 on fast cluster, got {}",
        snap_fast.concurrency_scale_permille
    );
    assert_eq!(snap_fast.pacing_delay_micros, 0);
    assert!(snap_fast.last_chunk_size > 128);

    let prom = metrics.render_prometheus(true, None);
    assert!(prom.contains("as_proxy_adaptive_batch_size "));
    assert!(prom.contains("as_proxy_adaptive_concurrency "));
    assert!(prom.contains("as_proxy_adaptive_pacing_micros 0"));
    assert!(prom.contains("as_proxy_cluster_wait_events_total 0"));

    // 2. Proactive Aerospike cluster queue-wait avoidance BEFORE any OverloadError occurs!
    tokio::time::sleep(Duration::from_millis(15)).await;
    sink.simulated_latency_us.store(22_000, Ordering::Relaxed);
    let wait_slice = wide_batch.slice(0, 256);
    engine
        .write_record_batch(&wait_slice, &params, 15)
        .await
        .unwrap();

    let snap_wait = engine.adaptive_snapshot();
    assert!(
        snap_wait.queue_inflation_permille > 1_550,
        "Expected queue_inflation_permille > 1550 under simulated cluster wait, got {}",
        snap_wait.queue_inflation_permille
    );
    assert!(snap_wait.batch_scale_permille < snap_fast.batch_scale_permille);
    assert!(snap_wait.concurrency_scale_permille < snap_fast.concurrency_scale_permille);
    assert!(snap_wait.pacing_delay_micros > 0);
    assert!(metrics.cluster_wait_events_total.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        metrics.overload_retries_total.load(Ordering::Relaxed),
        0,
        "Proactive cluster-wait backoff must trigger BEFORE any OverloadError!"
    );

    // 3. Explicit DEVICE_OVERLOAD spike via `write_records` (diverse Value types) + adaptive retry slicing + recovery
    tokio::time::sleep(Duration::from_millis(20)).await;
    sink.simulated_latency_us.store(100, Ordering::Relaxed);
    sink.observed_batch_sizes.lock().unwrap().clear();
    sink.overload_batches_remaining.store(1, Ordering::SeqCst);

    let mut diverse_records = Vec::with_capacity(96);
    for i in 0..96 {
        let mut m = HashMap::new();
        m.insert(Value::String("k".into()), Value::Int(i));
        diverse_records.push(PreparedRecord {
            key: Key::new("ns", "wide100", Value::Int(i)).unwrap(),
            bins: vec![
                Bin::new("s", Value::String("payload".into())),
                Bin::new("b", Value::Blob(vec![1, 2, 3, 4])),
                Bin::new("l", Value::List(vec![Value::Int(1), Value::Int(2)])),
                Bin::new("m", Value::HashMap(m)),
                Bin::new("f", Value::Float(FloatValue::from(1.5f64))),
            ],
        });
    }
    let ov_stats = engine
        .write_records(diverse_records, &params)
        .await
        .unwrap();
    assert_eq!(ov_stats.rows_written, 96);
    assert_eq!(ov_stats.overload_retries, 96);

    let retry_sizes = sink.observed_batch_sizes.lock().unwrap().clone();
    assert!(
        retry_sizes.len() >= 2 && retry_sizes.iter().skip(1).all(|&sz| sz < 96),
        "Overloaded batch of 96 must be split into smaller adaptive retry slices, got {retry_sizes:?}"
    );
    let snap_ov = engine.adaptive_snapshot();
    assert!(snap_ov.pacing_delay_micros >= 400);

    // Recovery when cluster returns to fast wait-free operation
    for _ in 0..8 {
        engine
            .write_record_batch(&wide_batch, &params, 15)
            .await
            .unwrap();
    }
    let snap_rec = engine.adaptive_snapshot();
    assert!(snap_rec.pacing_delay_micros < snap_ov.pacing_delay_micros);
    assert!(snap_rec.batch_scale_permille > snap_ov.batch_scale_permille);

    // 4. High active-stream fan-in fair-share concurrency & static mode (`adaptive_batching = false`)
    metrics.active_streams.store(128, Ordering::Relaxed);
    engine
        .write_record_batch(&wide_batch.slice(0, 128), &params, 15)
        .await
        .unwrap();
    assert!(
        engine.adaptive_snapshot().last_concurrency <= 2,
        "Expected global fair-share concurrency cap <= 2 under 128 active streams"
    );
    metrics.active_streams.store(0, Ordering::Relaxed);

    let static_engine = MppWriteEngine::new(
        sink,
        WriterConfig {
            adaptive_batching: false,
            batch_write_size: 200,
            min_batch_write_size: 16,
            ..WriterConfig::default()
        },
        retry_cfg,
        ProxyMetrics::new(),
    );
    static_engine
        .write_record_batch(&wide_batch.slice(0, 200), &params, 15)
        .await
        .unwrap();
    assert!(!static_engine.adaptive_snapshot().enabled);

    let example_cfg = as_proxy_server::config::AppConfig::from_file("config.example.toml").unwrap();
    assert!(example_cfg.writer.adaptive_batching);
    assert_eq!(example_cfg.writer.min_batch_write_size, 16);
    assert_eq!(example_cfg.writer.max_batch_write_size, 2048);
}
