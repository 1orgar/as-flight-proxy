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
use arrow_flight::{BasicAuth, FlightDescriptor, HandshakeRequest};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::Request;

use as_proxy_server::auth::Authenticator;
use as_proxy_server::config::{
    AuthConfig, RetryConfig, WriteMode, WriteParameters, WriterConfig,
};
use as_proxy_server::converter::{PreparedRecord, convert_record_batch};
use as_proxy_server::flight_service::{ProxyFlightService, PutBatchAck};
use as_proxy_server::metrics::ProxyMetrics;
use as_proxy_server::writer::{AerospikeSink, MppWriteEngine, RecordWriteOutcome};

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

    fn is_connected(&self) -> bool {
        true
    }
}

#[test]
fn test_convert_all_types_and_null_modes() {
    // Build a schema with:
    // - user_id (String, key)
    // - age (Int32)
    // - score (Float64)
    // - active (Boolean)
    // - amount (Decimal128(10, 2))
    // - birth_date (Date32 -> UTC ms)
    // - event_ts (Timestamp(Microsecond, UTC) -> UTC ms)
    // - raw_bytes (Binary)
    // - tags (List<Utf8>)
    // - props (Map<Utf8, Int64>)
    // - profile (Struct<city: Utf8, zip: Int32>)
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
        namespace: "test".to_string(),
        set_name: "users".to_string(),
        key_column: "user_id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: Some(3600),
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
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
    // Date32: 20_000 * 86_400_000 = 1_728_000_000_000 ms
    assert_eq!(
        r0.bins[4],
        Bin::new("birth_date", Value::Int(1_728_000_000_000))
    );
    // Timestamp(Microsecond): 1_700_000_000_123_456 / 1000 = 1_700_000_000_123 ms
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

    // 2. Replace mode omits null bins!
    let mut params_replace = params_update_nil.clone();
    params_replace.write_mode = WriteMode::Replace;
    let records_replace = convert_record_batch(&batch, &params_replace, 64).unwrap();
    assert_eq!(records_replace[1].bins.len(), 1);
    assert_eq!(
        records_replace[1].bins[0],
        Bin::new("score", Value::Float(FloatValue::from(50.0)))
    );
}

#[tokio::test]
async fn test_infinite_overload_retry_and_partial_batch_retry() {
    let k1 = Key::new("ns", "set", Value::Int(1)).unwrap();
    let k2 = Key::new("ns", "set", Value::Int(2)).unwrap();
    let k3 = Key::new("ns", "set", Value::Int(3)).unwrap();

    // k2 fails with DEVICE_OVERLOAD / KEY_BUSY 7 times in a row, while max_other_retries is only 2!
    // Because overload errors retry "до талого", k2 must succeed on attempt 8,
    // while k1 and k3 are written only on attempt 1!
    let mut overload_map = HashMap::new();
    overload_map.insert(k2.clone(), 7);

    let sink = Arc::new(MockAerospikeSink::new(overload_map, HashMap::new()));
    let metrics = ProxyMetrics::new();

    let writer_cfg = WriterConfig {
        use_batch_write: true,
        batch_write_size: 10,
        max_in_flight_records: 100,
        max_concurrent_subbatches_per_stream: 4,
    };
    let retry_cfg = RetryConfig {
        max_other_retries: 2, // Overload retry must ignore this cap and keep retrying!
        initial_backoff_ms: 1,
        max_backoff_ms: 4,
        backoff_multiplier: 1.5,
    };

    let engine = MppWriteEngine::new(
        sink.clone(),
        writer_cfg,
        retry_cfg,
        Arc::clone(&metrics),
    );

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
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
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
        namespace: "ns".to_string(),
        set_name: "set".to_string(),
        key_column: "id".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: None,
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
    };

    let err = engine.write_records(records, &params).await.unwrap_err();
    let attempts = sink.write_attempts.lock().unwrap();
    // 1 initial + 3 retries = 4 attempts total
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
        namespace: "prod_ns".to_string(),
        set_name: "features".to_string(),
        key_column: "key_col".to_string(),
        write_mode: WriteMode::Update,
        ttl_seconds: Some(7200),
        send_key: true,
        ignore_nulls: false,
        use_batch_write: Some(true),
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

    // Verify all 50 keys were written to the mock sink and key 10 has Value::Nil for bin_val
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
