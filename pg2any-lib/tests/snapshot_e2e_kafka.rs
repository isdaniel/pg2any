//! Initial-snapshot end-to-end scenarios: live PostgreSQL source, Kafka destination.
//!
//! Needs running PostgreSQL and Kafka (see `docker-compose.snapshot-test.yml`, profile `kafka`);
//! run with `make snapshot-test-kafka`. The tests must not run in parallel (`--test-threads=1`,
//! as the Makefile does): the topic prefix is a process-wide environment variable. The broker
//! (`PG2ANY_TEST_KAFKA_BROKERS`) must be a dedicated test broker: every test starts by deleting the
//! topics earlier runs of this file generated (`snaptest<hex>g<N>.<schema>.<table>`).
//!
//! Kafka is the odd one out. A "table" is a one-partition topic `<prefix>.<schema>.<table>`; rows
//! are Debezium-style JSON messages (snapshot rows `op = "r"`, live ones `c`/`u`/`d`); nothing can
//! be un-published, so the snapshot target policy is a no-op and a restarted snapshot publishes its
//! rows again (at-least-once). The harness `Target` therefore REPLAYS a topic: the latest message
//! per row id wins, a `d` removes the row. Replay identifies a row by the id inside the payload, not
//! by the message key: a live INSERT has no key until the handler has seen an UPDATE/DELETE of the
//! table (a limit of the live path that predates the snapshot), while snapshot, update and delete
//! messages are keyed (checked separately by the wrappers below).
#![cfg(feature = "kafka")]

#[allow(dead_code)]
mod snapshot_common;

use async_trait::async_trait;
use chrono::Utc;
use pg2any_lib::destinations::destination_factory::DestinationHandler;
use pg2any_lib::{Config, DestinationType, SnapshotTargetPolicy};
use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use snapshot_common::snapshot_harness::{
    client_config, start_client, Digest, PgFixture, Scenario, TableSet, Target, MIXED_CASE_TABLE,
    NULL_TEXT,
};
use snapshot_common::snapshot_scenarios::{
    self, assert_mentions, complete, fail_with, fresh_items, marker_bytes, start_with,
    write_marker, LIVE_ROW_TIMEOUT, SENTINEL_ID, SMALL_ROWS, SMALL_SNAPSHOT_TIMEOUT,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const DEFAULT_BROKERS: &str = "127.0.0.1:9094";
/// Topics of every earlier run start with this; `KafkaTarget::new` deletes them.
const TOPIC_ROOT: &str = "snaptest";

/// What the producer (`CDC_KAFKA_MESSAGE_MAX_BYTES`, librdkafka's `message.max.bytes`) and the
/// test topics (`max.message.bytes`) accept; the compose broker is set to the same. The Kafka
/// default of ~1 MB cannot carry a 512 KiB bytea (one MiB of hex text in JSON).
const MESSAGE_MAX_BYTES: usize = 200 * 1024 * 1024;
/// `CDC_KAFKA_LINGER_MS` of the test process (the destination's default is 5). S1 applies its
/// ~11,000 queued live transactions one commit at a time, each waiting out the linger: at 5 ms the
/// backlog drained at ~100 tx/s and S1 converged 95-121 s after the writer stopped (bound 300 s),
/// at 0 ms at ~220 tx/s in 55 s. The snapshot delivers whole batches and slows by ~20% (S1's copy
/// 56 -> 68 s, S10 80 -> 102 s).
const LINGER_MS: u32 = 0;
/// librdkafka drops the connection to a broker whose fetch response is bigger than this (default
/// 100,000,000), so a consumer of a message of `MESSAGE_MAX_BYTES` has to raise it.
const RECEIVE_MESSAGE_MAX_BYTES: usize = 400 * 1024 * 1024;

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
/// A new topic is visible within a second or two; the first one on a freshly started broker is not.
const TOPIC_CREATION_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_SLICE: Duration = Duration::from_millis(500);
/// Bounds reading a topic up to its high watermark (a 130 MB topic takes a few seconds).
const READ_TIMEOUT: Duration = Duration::from_secs(180);

/// Column order of the standard tables, as the PostgreSQL DDL declares it (JSON objects are not
/// ordered, `Target::row` is).
fn columns(table: &str) -> &'static [&'static str] {
    match table {
        "snap_items" => &["id", "name", "payload", "note"],
        "snap_wide" => &["id", "blob"],
        "snap_part" => &["id", "label"],
        "snap_empty" => &["id", "v"],
        "snap_special" => &["id", "order", "Mixed Case", "raw"],
        MIXED_CASE_TABLE => &["id", "group"],
        other => panic!("unknown standard table {other}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------------------------

/// The parts of an envelope replay needs; serde skips every other field (the big text cells too)
/// without allocating for it.
#[derive(Deserialize)]
struct Envelope {
    payload: Payload,
}

#[derive(Deserialize)]
struct Payload {
    op: String,
    before: Option<RowId>,
    after: Option<RowId>,
    source: SourceBlock,
}

#[derive(Deserialize)]
struct RowId {
    id: Option<String>,
}

#[derive(Deserialize)]
struct SourceBlock {
    lsn: Option<u64>,
}

#[derive(Deserialize)]
struct KeyEnvelope {
    payload: RowId,
}

#[derive(Deserialize)]
struct FullPayload {
    after: Option<HashMap<String, Option<String>>>,
}

#[derive(Deserialize)]
struct FullEnvelope {
    payload: FullPayload,
}

#[derive(Clone, Debug)]
struct Msg {
    offset: i64,
    op: char,
    /// The id of the row the message is about (`after` for r/c/u, `before` for d).
    id: Option<i64>,
    /// An `u` whose key changed names the row it replaces.
    replaced_id: Option<i64>,
    lsn: Option<u64>,
    /// The raw message key, kept only for audits.
    key: Option<String>,
}

fn parse_id(text: &str) -> i64 {
    text.parse()
        .unwrap_or_else(|_| panic!("a row id is a decimal integer, got {text:?}"))
}

fn parse_message(offset: i64, key: Option<&[u8]>, payload: &[u8], keep_key: bool) -> Msg {
    let envelope: Envelope = serde_json::from_slice(payload)
        .unwrap_or_else(|e| panic!("message at offset {offset} is not an envelope: {e}"));
    let payload = envelope.payload;
    let op = payload.op.chars().next().expect("op");
    let before = payload.before.and_then(|r| r.id).map(|id| parse_id(&id));
    let after = payload.after.and_then(|r| r.id).map(|id| parse_id(&id));
    let (id, replaced_id) = match op {
        'd' => (before, None),
        'u' => (after, before.filter(|b| Some(*b) != after)),
        _ => (after, None),
    };
    let key = key.map(|k| String::from_utf8(k.to_vec()).expect("a UTF-8 key"));
    if let (Some(key), Some(id)) = (&key, id) {
        // A key that does not hold `id` (a table keyed by another column) has nothing to check.
        let key_id: KeyEnvelope = serde_json::from_str(key).expect("a JSON key");
        if let Some(key_id) = key_id.payload.id {
            assert_eq!(
                parse_id(&key_id),
                id,
                "the key of the message at offset {offset} names another row than its payload"
            );
        }
    }
    Msg {
        offset,
        op,
        id,
        replaced_id,
        lsn: payload.source.lsn,
        key: if keep_key { key } else { None },
    }
}

/// Latest message offset per surviving row id.
#[derive(Default)]
struct Replay {
    next: i64,
    live: HashMap<i64, i64>,
}

impl Replay {
    fn apply(&mut self, m: &Msg) {
        match m.op {
            'r' | 'c' | 'u' => {
                if let Some(old) = m.replaced_id {
                    self.live.remove(&old);
                }
                self.live
                    .insert(m.id.expect("an upsert names its row"), m.offset);
            }
            'd' => {
                self.live.remove(&m.id.expect("a delete names its row"));
            }
            't' => self.live.clear(),
            other => panic!("unexpected op {other:?} at offset {}", m.offset),
        }
    }

    fn digest(&self) -> Digest {
        Digest {
            count: self.live.len() as u64,
            min_id: self.live.keys().copied().min().unwrap_or(0),
            max_id: self.live.keys().copied().max().unwrap_or(0),
            sum_id: self.live.keys().sum(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Reading topics
// ---------------------------------------------------------------------------------------------

/// One blocking consumer. It never joins a group: every read `assign`s a partition at an explicit
/// offset, so no offsets are committed and `group.id` only has to exist. The queue and fetch
/// sizes are small so reading does not add memory to the S11 measurements.
struct Reader {
    consumer: BaseConsumer,
    replay: HashMap<String, Replay>,
}

impl Reader {
    fn new(brokers: &str) -> Self {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "group.id",
                format!("snapshot-e2e-reader-{}", unique_suffix()),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("fetch.message.max.bytes", "16384")
            .set("fetch.max.bytes", "65536")
            .set("message.max.bytes", "65536")
            .set(
                "receive.message.max.bytes",
                RECEIVE_MESSAGE_MAX_BYTES.to_string(),
            )
            .set("queued.max.messages.kbytes", "4096")
            .set("queued.min.messages", "100")
            .set("fetch.wait.max.ms", "10")
            .set("fetch.queue.backoff.ms", "5")
            .create()
            .expect("Kafka consumer");
        Self {
            consumer,
            replay: HashMap::new(),
        }
    }

    /// The topic's high watermark, or `None` while the topic does not exist.
    fn high_watermark(&self, topic: &str) -> Option<i64> {
        match self.consumer.fetch_watermarks(topic, 0, METADATA_TIMEOUT) {
            Ok((_, high)) => Some(high),
            Err(e)
                if matches!(
                    e.rdkafka_error_code(),
                    Some(
                        RDKafkaErrorCode::UnknownTopicOrPartition
                            | RDKafkaErrorCode::UnknownPartition
                    )
                ) =>
            {
                None
            }
            Err(e) => panic!("watermarks of {topic}: {e}"),
        }
    }

    /// Waits until the broker lists the new topic with its partition. Asking for the topic's
    /// metadata is what makes librdkafka look again: `fetch_watermarks` alone keeps answering from
    /// the "unknown topic" it cached when the topic was not there yet.
    fn wait_for_topic(&self, topic: &str) {
        let deadline = Instant::now() + TOPIC_CREATION_TIMEOUT;
        loop {
            let listed = self
                .consumer
                .fetch_metadata(Some(topic), METADATA_TIMEOUT)
                .map(|m| {
                    m.topics().iter().any(|t| {
                        t.name() == topic && t.error().is_none() && !t.partitions().is_empty()
                    })
                });
            if matches!(listed, Ok(true)) && self.high_watermark(topic).is_some() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "topic {topic} never appeared: {listed:?}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Hands every message from `from` up to the high watermark at call time to `each`; returns
    /// that watermark (`from` for a missing topic).
    fn read(&self, topic: &str, from: i64, mut each: impl FnMut(&BorrowedMessage<'_>)) -> i64 {
        let Some(high) = self.high_watermark(topic) else {
            return from;
        };
        if from >= high {
            return from;
        }
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, 0, Offset::Offset(from))
            .expect("partition offset");
        self.consumer.assign(&assignment).expect("assign");
        let deadline = Instant::now() + READ_TIMEOUT;
        let (mut next, mut last_error) = (from, None);
        while next < high {
            match self.consumer.poll(POLL_SLICE) {
                Some(Ok(message)) => {
                    each(&message);
                    next = message.offset() + 1;
                }
                Some(Err(e)) => last_error = Some(e),
                None => {}
            }
            assert!(
                Instant::now() < deadline,
                "reading {topic} stalled at offset {next} of {high}: {last_error:?}"
            );
        }
        high
    }

    /// Brings the replay of `topic` up to the high watermark and returns it.
    fn catch_up(&mut self, topic: &str) -> &Replay {
        let mut replay = self.replay.remove(topic).unwrap_or_default();
        replay.next = self.read(topic, replay.next, |m| {
            let payload = m.payload().expect("a message with a payload");
            replay.apply(&parse_message(m.offset(), m.key(), payload, false));
        });
        self.replay.entry(topic.to_string()).or_insert(replay)
    }

    /// The payload of the one message at `offset`.
    fn message_at(&self, topic: &str, offset: i64) -> Vec<u8> {
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, 0, Offset::Offset(offset))
            .expect("partition offset");
        self.consumer.assign(&assignment).expect("assign");
        let deadline = Instant::now() + READ_TIMEOUT;
        let mut last_error = None;
        loop {
            match self.consumer.poll(POLL_SLICE) {
                Some(Ok(m)) if m.offset() == offset => {
                    return m.payload().expect("a payload").to_vec()
                }
                Some(Ok(m)) => panic!("asked for offset {offset} of {topic}, got {}", m.offset()),
                Some(Err(e)) => last_error = Some(e),
                None => {}
            }
            assert!(
                Instant::now() < deadline,
                "no message at offset {offset} of {topic}: {last_error:?}"
            );
        }
    }

    /// Every message of the topic, in order (a full read, independent of the replay cache).
    fn audit(&self, topic: &str) -> Vec<Msg> {
        let mut messages = Vec::new();
        self.read(topic, 0, |m| {
            let payload = m.payload().expect("a message with a payload");
            messages.push(parse_message(m.offset(), m.key(), payload, true));
        });
        messages
    }
}

/// The topic prefix of a target: `snaptest<hex>` is the target's own, `g<N>` its generation.
fn new_base_prefix() -> String {
    format!("{TOPIC_ROOT}{}", unique_suffix())
}

fn generation_prefix(base_prefix: &str, generation: u32) -> String {
    format!("{base_prefix}g{generation}")
}

/// Whether `name` is a topic made by `generation_prefix(new_base_prefix(), N)`: nothing else on
/// the broker is ever deleted.
fn is_generated_topic(name: &str) -> bool {
    let Some((hex, rest)) = name
        .strip_prefix(TOPIC_ROOT)
        .and_then(|r| r.split_once('g'))
    else {
        return false;
    };
    let Some((generation, _)) = rest.split_once('.') else {
        return false;
    };
    let all = |s: &str, digit: fn(&u8) -> bool| !s.is_empty() && s.bytes().all(|b| digit(&b));
    all(hex, u8::is_ascii_hexdigit) && all(generation, u8::is_ascii_digit)
}

#[test]
fn only_generated_topics_are_matched_for_deletion() {
    let mine = format!(
        "{}.snap_s1_kafka.snap_items",
        generation_prefix(&new_base_prefix(), 12)
    );
    assert!(is_generated_topic(&mine), "{mine}");
    for other in [
        "__consumer_offsets",
        "snaptest",
        "snaptestg1.x.y",
        "snaptestabcg.x.y",
        "snaptestabcg1",
        "snaptestxyzg1.x.y",
        "pg2any.public.users",
        "other.snaptest18ebg1.x.y",
        "snaptest-orders.public.t",
    ] {
        assert!(!is_generated_topic(other), "{other}");
    }
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{nanos:x}")
}

/// Everything the wrappers need to know about a topic's messages.
struct Audit(Vec<Msg>);

impl Audit {
    fn count(&self, op: char) -> usize {
        self.0.iter().filter(|m| m.op == op).count()
    }

    fn of(&self, op: char) -> impl Iterator<Item = &Msg> {
        self.0.iter().filter(move |m| m.op == op)
    }

    fn snapshot_points(&self) -> BTreeSet<u64> {
        self.of('r')
            .map(|m| m.lsn.expect("a snapshot message carries the point"))
            .collect()
    }

    fn distinct_snapshot_ids(&self) -> usize {
        self.of('r')
            .map(|m| m.id.expect("id"))
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn by_op(&self) -> BTreeMap<char, usize> {
        let mut counts = BTreeMap::new();
        for m in &self.0 {
            *counts.entry(m.op).or_insert(0) += 1;
        }
        counts
    }
}

// ---------------------------------------------------------------------------------------------
// The target
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct State {
    /// Bumped by `reset`: every generation has its own topic prefix, so "dropping the tables" is
    /// starting over on fresh topics (a topic cannot be reliably re-created right after deletion).
    generation: u32,
    schema: Option<String>,
    tables: Vec<String>,
}

struct KafkaTarget {
    brokers: String,
    base_prefix: String,
    reader: Arc<StdMutex<Reader>>,
    /// `max.message.bytes` of the topics `create_tables` makes.
    topic_max_message_bytes: AtomicUsize,
    state: StdMutex<State>,
    /// The library's own destination, for sentinel messages and injected snapshot rows.
    writer: Mutex<Option<Box<dyn DestinationHandler>>>,
}

impl KafkaTarget {
    async fn new() -> Self {
        Self::with_message_max_bytes(MESSAGE_MAX_BYTES).await
    }

    /// The limit is read by every handler when it connects.
    fn set_message_max_bytes(&self, bytes: usize) {
        std::env::set_var("CDC_KAFKA_MESSAGE_MAX_BYTES", bytes.to_string());
    }

    async fn with_message_max_bytes(message_max_bytes: usize) -> Self {
        let brokers = std::env::var("PG2ANY_TEST_KAFKA_BROKERS")
            .unwrap_or_else(|_| DEFAULT_BROKERS.to_string());
        std::env::set_var("CDC_KAFKA_MESSAGE_MAX_BYTES", message_max_bytes.to_string());
        std::env::set_var("CDC_KAFKA_LINGER_MS", LINGER_MS.to_string());
        let target = Self {
            base_prefix: new_base_prefix(),
            reader: {
                let brokers = brokers.clone();
                Arc::new(StdMutex::new(
                    tokio::task::spawn_blocking(move || Reader::new(&brokers))
                        .await
                        .expect("reader"),
                ))
            },
            brokers,
            topic_max_message_bytes: AtomicUsize::new(MESSAGE_MAX_BYTES),
            state: StdMutex::new(State::default()),
            writer: Mutex::new(None),
        };
        target.delete_earlier_topics().await;
        std::env::set_var("CDC_KAFKA_TOPIC_PREFIX", target.prefix());
        target
    }

    fn prefix(&self) -> String {
        generation_prefix(&self.base_prefix, self.state.lock().unwrap().generation)
    }

    fn topic(&self, table: &str) -> String {
        let schema = self
            .state
            .lock()
            .unwrap()
            .schema
            .clone()
            .expect("create_tables names the schema before any read");
        format!("{}.{schema}.{table}", self.prefix())
    }

    fn admin(&self) -> AdminClient<DefaultClientContext> {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("Kafka admin client")
    }

    /// Names of the topics of earlier runs (and of this one).
    async fn earlier_topics(&self) -> Vec<String> {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let reader = reader.lock().unwrap();
            let metadata = reader
                .consumer
                .fetch_metadata(None, METADATA_TIMEOUT)
                .expect("Kafka metadata");
            metadata
                .topics()
                .iter()
                .map(|t| t.name().to_string())
                .filter(|name| is_generated_topic(name))
                .collect()
        })
        .await
        .expect("metadata task")
    }

    /// Topics of earlier runs would pile up in a container that is never removed. The broker
    /// deletes asynchronously (a big topic takes a while), and new topics queue behind that, so
    /// this waits until the old ones are gone.
    async fn delete_earlier_topics(&self) {
        let old = self.earlier_topics().await;
        if old.is_empty() {
            return;
        }
        let names: Vec<&str> = old.iter().map(String::as_str).collect();
        let _ = self
            .admin()
            .delete_topics(&names, &AdminOptions::new())
            .await;
        let deadline = Instant::now() + Duration::from_secs(120);
        while !self.earlier_topics().await.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the topics of earlier runs are still there"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Changes `max.message.bytes` of an existing topic.
    async fn alter_topic_limit(&self, table: &str, bytes: usize) {
        let topic = self.topic(table);
        let bytes = bytes.to_string();
        let mut config = AlterConfig::new(ResourceSpecifier::Topic(&topic));
        config.entries.insert("max.message.bytes", &bytes);
        let results = self
            .admin()
            .alter_configs(&[config], &AdminOptions::new())
            .await
            .expect("alter topic config");
        for result in results {
            result.unwrap_or_else(|(resource, code)| panic!("alter {resource:?}: {code}"));
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Reader) -> T + Send + 'static,
    ) -> T {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || f(&mut reader.lock().unwrap()))
            .await
            .expect("blocking Kafka task panicked")
    }

    /// The messages of `table`'s topic, in order.
    async fn audit(&self, table: &str) -> Audit {
        let topic = self.topic(table);
        Audit(self.blocking(move |r| r.audit(&topic)).await)
    }

    async fn new_handler(&self) -> Box<dyn DestinationHandler> {
        let cfg = Config::builder()
            .source_connection_string("postgresql://unused")
            .destination_type(DestinationType::Kafka)
            .destination_connection_string(&self.brokers)
            .replication_slot_name("unused")
            .publication_name("unused")
            .build()
            .expect("valid config");
        let mut handler = cfg.create_destination().expect("Kafka destination");
        handler
            .connect(&cfg.destination_connection_string)
            .await
            .expect("connect to the compose Kafka");
        handler
    }

    /// Publishes rows `ids` of `table` exactly as a snapshot that crashed after them would have:
    /// through the destination's own snapshot path, stamped with that snapshot's `point`.
    async fn publish_snapshot_rows(
        &self,
        pg: &PgFixture,
        s: &Scenario,
        table: &str,
        ids: std::ops::RangeInclusive<i64>,
        point: u64,
    ) {
        let names = columns(table);
        let select: Vec<String> = names.iter().map(|c| format!("\"{c}\"")).collect();
        let rows = pg
            .query(&format!(
                "SELECT {} FROM \"{}\".\"{table}\" WHERE id BETWEEN {} AND {} ORDER BY id",
                select.join(", "),
                s.schema,
                ids.start(),
                ids.end()
            ))
            .await;
        assert_eq!(
            rows.len() as i64,
            ids.end() - ids.start() + 1,
            "source rows"
        );
        let events: Vec<ChangeEvent> = rows
            .iter()
            .map(|row| {
                let pairs: Vec<(&str, ColumnValue)> = names
                    .iter()
                    .zip(row)
                    .map(|(name, cell)| {
                        let value = cell.as_deref().map_or(ColumnValue::Null, ColumnValue::text);
                        (*name, value)
                    })
                    .collect();
                ChangeEvent::insert(
                    s.schema.as_str(),
                    table,
                    1,
                    RowData::from_pairs(pairs),
                    Lsn(point),
                )
            })
            .collect();
        let mut handler = self.new_handler().await;
        handler.set_snapshot_key_columns(&format!("{}.{table}", s.schema), vec![Arc::from("id")]);
        handler.begin_snapshot_writes();
        handler
            .execute_snapshot_events_batch(&events, Lsn(point))
            .await
            .expect("publish the pre-crash snapshot rows");
        handler.close().await.expect("close");
    }
}

#[async_trait]
impl Target for KafkaTarget {
    fn dest_type(&self) -> DestinationType {
        DestinationType::Kafka
    }

    fn connection_string(&self) -> String {
        self.brokers.clone()
    }

    async fn create_tables(&self, s: &Scenario, which: &[&str]) {
        {
            let mut state = self.state.lock().unwrap();
            state.schema = Some(s.schema.clone());
            for table in which {
                if !state.tables.iter().any(|t| t == table) {
                    state.tables.push(table.to_string());
                }
            }
        }
        let names: Vec<String> = which.iter().map(|t| self.topic(t)).collect();
        let max_message_bytes = self
            .topic_max_message_bytes
            .load(Ordering::Relaxed)
            .to_string();
        let topics: Vec<NewTopic> = names
            .iter()
            .map(|name| {
                NewTopic::new(name, 1, TopicReplication::Fixed(1))
                    .set("max.message.bytes", &max_message_bytes)
            })
            .collect();
        let created = self
            .admin()
            .create_topics(&topics, &AdminOptions::new())
            .await
            .expect("create topics");
        for result in created {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((name, code)) => panic!("could not create topic {name}: {code}"),
            }
        }
        self.blocking(move |r| names.iter().for_each(|n| r.wait_for_topic(n)))
            .await;
    }

    async fn digest(&self, table: &str) -> Digest {
        let topic = self.topic(table);
        self.blocking(move |r| r.catch_up(&topic).digest()).await
    }

    async fn row(&self, table: &str, id: i64) -> Option<Vec<String>> {
        let topic = self.topic(table);
        let message = self
            .blocking(move |r| {
                let offset = r.catch_up(&topic).live.get(&id).copied()?;
                Some(r.message_at(&topic, offset))
            })
            .await?;
        let envelope: FullEnvelope = serde_json::from_slice(&message).expect("an envelope");
        let mut after = envelope.payload.after.expect("a row message has `after`");
        let names = columns(table);
        assert_eq!(
            after.len(),
            names.len(),
            "{table} id {id}: the message has other columns than the table"
        );
        Some(
            names
                .iter()
                .map(|name| match after.remove(*name) {
                    Some(Some(text)) => text,
                    Some(None) => NULL_TEXT.to_string(),
                    None => panic!("{table} id {id}: column {name} missing from the message"),
                })
                .collect(),
        )
    }

    async fn insert_sentinel(&self, table: &str, id: i64) {
        let schema = self.state.lock().unwrap().schema.clone().expect("schema");
        let pairs: Vec<(&str, ColumnValue)> = columns(table)
            .iter()
            .map(|name| {
                let value = if *name == "id" {
                    ColumnValue::text(&id.to_string())
                } else {
                    ColumnValue::Null
                };
                (*name, value)
            })
            .collect();
        let event = ChangeEvent::insert(
            schema.as_str(),
            table,
            1,
            RowData::from_pairs(pairs),
            Lsn(1),
        );
        let mut writer = self.writer.lock().await;
        if writer.is_none() {
            *writer = Some(self.new_handler().await);
        }
        writer
            .as_mut()
            .expect("handler")
            .execute_events_batch_with_hook(&[event], 1, Utc::now(), Some(Lsn(1)), None)
            .await
            .expect("publish the sentinel");
    }

    /// Starts over on fresh topics: a new generation of the topic prefix. The next client or
    /// handler reads the prefix when it connects.
    async fn reset(&self) {
        if let Some(mut handler) = self.writer.lock().await.take() {
            handler.close().await.expect("close");
        }
        {
            let mut state = self.state.lock().unwrap();
            state.generation += 1;
            state.schema = None;
            state.tables.clear();
        }
        std::env::set_var("CDC_KAFKA_TOPIC_PREFIX", self.prefix());
        self.reader.lock().unwrap().replay.clear();
    }

    /// Measured by S11b: the 64 MB row (61.04 MiB; a 128 MB JSON message) peaked 488.8-493.4 MiB
    /// over eight runs, 8.0-8.1x its size, with the same figure for this compressible row and for
    /// 64 MB of random bytes (measured separately: hex hardly compresses, the batch was 119 MB). Four copies of the 2x hex
    /// text coexist: the COPY-decoded cell, the JSON value built from it, the serialized message
    /// and librdkafka's own copy of it. Rounded up to 9 (~11% headroom, the runs differ by <1%).
    fn row_amplification(&self) -> u64 {
        9
    }

    // `buffer_copies` keeps the default (4): S11a measured 22.7-36.1 MiB over eight runs against
    // its 76 MiB bound (SQLite 41-55 MiB). Each batch (at most half the 8 MiB buffer, one writer)
    // is held as events and, once enqueued, as librdkafka's copies until its deliveries are
    // awaited; the batch is the backpressure, so the producer queue never grows past it.

    /// The benchmark copy is one `r` message per row: the topic holds exactly `rows` messages and
    /// its first and last are snapshot reads. The topics are deleted afterwards (a point leaves up
    /// to a few GB behind).
    async fn on_bench_completed(&self, table: &str, rows: u64, _batch_rows: usize) {
        let topic = self.topic(table);
        self.blocking(move |r| {
            let high = r.high_watermark(&topic).expect("the topic exists");
            assert_eq!(high as u64, rows, "one message per snapshot row");
            for offset in [0, high - 1] {
                let m = parse_message(offset, None, &r.message_at(&topic, offset), false);
                assert_eq!(m.op, 'r', "message {offset} is a snapshot read");
            }
        })
        .await;
        self.delete_earlier_topics().await;
    }
}

// ---------------------------------------------------------------------------------------------
// Shared scenarios
// ---------------------------------------------------------------------------------------------

macro_rules! kafka_scenario {
    ($test:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires live services; run via make snapshot-test-*"]
        async fn $test() {
            snapshot_scenarios::$scenario(&KafkaTarget::new().await).await;
        }
    };
}

kafka_scenario!(kafka_s3, s3_existing_slot_without_marker_fails_fast);
kafka_scenario!(kafka_s8, s8_empty_tables_and_empty_publication);
kafka_scenario!(kafka_s9, s9_special_identifiers_and_values);
kafka_scenario!(kafka_s13, s13_kill_mid_stream_then_restart_converges);
kafka_scenario!(kafka_s14, s14_graceful_stop_mid_stream_applies_everything);
kafka_scenario!(
    kafka_s15,
    s15_small_transaction_backlog_converges_and_the_tail_is_not_held_back
);

// S2 and S7 (target policy, missing destination tables) do not apply: a Kafka topic is created on
// demand and cannot be probed or truncated.

/// Rows the snapshot copied (S1 seeds 200,000 `snap_items`, 1,000 `snap_wide`, 20,000 `snap_part`),
/// not counting the few writer rows that committed before the slot existed.
const S1_SEEDED_ITEMS: usize = 200_000;
const S1_SEEDED_WIDE: usize = 1_000;
const S1_SEEDED_PART: usize = 20_000;
/// S1's writer inserts at least this many rows before and after the handoff.
const S1_MIN_LIVE_INSERTS: usize = 500;

/// S1 plus what only Kafka can show: the topic holds every snapshot message (`op = "r"`, all with
/// the same consistent point, distinct keys) before any live message (`c`/`u`/`d`, all after
/// that point), and an update or delete carries the very key its snapshot message had.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_s1() {
    let target = KafkaTarget::new().await;
    snapshot_scenarios::s1_consistent_handoff_under_writes(&target).await;
    let items = target.audit("snap_items").await;
    let points = items.snapshot_points();
    assert_eq!(
        points.len(),
        1,
        "one consistent point for the whole snapshot"
    );
    let point = *points.iter().next().expect("snapshot messages");

    for (table, seeded) in [
        ("snap_wide", S1_SEEDED_WIDE),
        ("snap_part", S1_SEEDED_PART),
        ("snap_empty", 0),
    ] {
        let audit = target.audit(table).await;
        assert_eq!(
            audit.0.len(),
            seeded,
            "{table}: one snapshot message per row"
        );
        assert_eq!(audit.count('r'), seeded, "{table}: only snapshot messages");
        assert!(
            audit.snapshot_points().iter().all(|p| *p == point),
            "{table}: snapshot messages carry the consistent point {point}"
        );
        assert_eq!(audit.distinct_snapshot_ids(), seeded);
        // `snap_part` is a partitioned table published through its root: its key resolves too.
        assert!(
            audit
                .of('r')
                .all(|m| m.key.as_deref().is_some_and(|k| key_fields(k) == ["id"])),
            "{table}: every snapshot message is keyed by the primary key column"
        );
    }

    println!("S1 snap_items messages by op: {:?}", items.by_op());
    let snapshot_rows = items.count('r');
    assert!(
        snapshot_rows >= S1_SEEDED_ITEMS,
        "{snapshot_rows} snapshot messages for {S1_SEEDED_ITEMS} seeded rows"
    );
    assert_eq!(
        items.distinct_snapshot_ids(),
        snapshot_rows,
        "no row copied twice"
    );
    assert!(
        items.0[..snapshot_rows].iter().all(|m| m.op == 'r'),
        "all snapshot messages precede every live message"
    );
    assert!(
        items.0[snapshot_rows..].iter().all(|m| m.op != 'r'),
        "no snapshot message after the handoff"
    );
    assert!(
        items.0[snapshot_rows..]
            .iter()
            .all(|m| m.lsn.expect("a live message carries its commit LSN") > point),
        "live messages commit after the consistent point"
    );

    let (inserts, updates, deletes) = (items.count('c'), items.count('u'), items.count('d'));
    assert!(
        inserts >= S1_MIN_LIVE_INSERTS && updates >= 100 && deletes >= 20,
        "live messages: {inserts} c, {updates} u, {deletes} d"
    );
    assert_eq!(
        snapshot_rows + inserts + updates + deletes,
        items.0.len(),
        "no other op"
    );

    let snapshot_keys: HashMap<i64, &str> = items
        .of('r')
        .map(|m| {
            (
                m.id.expect("id"),
                m.key.as_deref().expect("a snapshot message is keyed"),
            )
        })
        .collect();
    assert!(
        snapshot_keys.values().all(|key| key_fields(key) == ["id"]),
        "a snapshot key is made of the primary key column"
    );
    for m in items.of('u').chain(items.of('d')) {
        let id = m.id.expect("id");
        assert_eq!(
            Some(m.key.as_deref().expect("an update/delete is keyed")),
            snapshot_keys.get(&id).copied(),
            "the {} message of id {id} has another key than its snapshot message",
            m.op
        );
    }
    // The live handler is given every published table's replica-identity key at start, so a live
    // INSERT is keyed like the snapshot row and the UPDATE/DELETE that follow it.
    let unkeyed_inserts = items.of('c').filter(|m| m.key.is_none()).count();
    assert_eq!(
        unkeyed_inserts, 0,
        "{unkeyed_inserts} of {inserts} live inserts have no key"
    );
    for m in items.of('c') {
        assert_eq!(
            key_fields(m.key.as_deref().expect("keyed")),
            ["id"],
            "a live insert is keyed by the primary key column"
        );
    }
}

/// S5 plus: the restart did not publish the snapshot again (one message per row).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_s5() {
    let target = KafkaTarget::new().await;
    snapshot_scenarios::s5_completed_marker_skips_snapshot_on_restart(&target).await;
    let items = target.audit("snap_items").await;
    assert_eq!(
        items.0.len(),
        SMALL_ROWS as usize + 2,
        "{:?}: the snapshot rows, the sentinel and the live row, each once",
        items.by_op()
    );
    assert_eq!(items.count('r'), SMALL_ROWS as usize);
}

/// S10 plus: the cancelled attempt's rows stay in the topic, so the restart duplicates them: two
/// snapshot points, more messages than rows, one distinct id per row.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_s10_cancel_mid_copy_duplicates_are_allowed() {
    const ROWS: usize = 300_000;
    let target = KafkaTarget::new().await;
    snapshot_scenarios::s10_cancel_mid_copy_then_restart_completes(&target).await;
    let items = target.audit("snap_items").await;
    println!(
        "S10 messages {}, distinct ids {}, points {:?}",
        items.0.len(),
        items.distinct_snapshot_ids(),
        items.snapshot_points()
    );
    assert_eq!(items.distinct_snapshot_ids(), ROWS, "every row is there");
    assert!(
        items.0.len() > ROWS && items.0.len() <= 2 * ROWS && items.0.len() == items.count('r'),
        "the cancelled attempt's rows were published too, and only a part of the table twice"
    );
    assert_eq!(
        items.snapshot_points().len(),
        2,
        "the messages come from two snapshots"
    );
}

// ---------------------------------------------------------------------------------------------
// S4 and S6 for a destination that cannot clean up
// ---------------------------------------------------------------------------------------------

/// The key columns a message key names, in the key's own order.
fn key_fields(key: &str) -> Vec<String> {
    let key: serde_json::Value = serde_json::from_str(key).expect("a JSON key");
    key["schema"]["fields"]
        .as_array()
        .expect("key fields")
        .iter()
        .map(|f| f["field"].as_str().expect("field name").to_string())
        .collect()
}

/// A snapshot message carries the key a live UPDATE of the same row carries, whatever the table's
/// replica identity: the primary key (declared in another order than the columns, so the order
/// of the key's fields is the relation's), a chosen unique index, all columns (FULL); a table
/// without any key columns has unkeyed snapshot messages.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_snapshot_keys_follow_the_replica_identity() {
    const TABLES: [&str; 4] = ["k_composite", "k_index", "k_full", "k_none"];
    let target = KafkaTarget::new().await;
    let pg = PgFixture::connect().await;
    let sc = pg.reset("keys_kafka").await;
    target.reset().await;
    let q = &sc.schema;
    for ddl in [
        format!("CREATE TABLE {q}.k_composite (tenant text NOT NULL, id bigint NOT NULL, v text, PRIMARY KEY (id, tenant))"),
        format!("CREATE TABLE {q}.k_index (id bigint NOT NULL, code text NOT NULL, v text)"),
        format!("CREATE UNIQUE INDEX k_index_code ON {q}.k_index (code)"),
        format!("ALTER TABLE {q}.k_index REPLICA IDENTITY USING INDEX k_index_code"),
        format!("CREATE TABLE {q}.k_full (id bigint, v text)"),
        format!("ALTER TABLE {q}.k_full REPLICA IDENTITY FULL"),
        format!("CREATE TABLE {q}.k_none (id bigint, v text)"),
        format!("INSERT INTO {q}.k_composite VALUES ('acme', 7, 'a')"),
        format!("INSERT INTO {q}.k_index VALUES (7, 'c-7', 'a')"),
        format!("INSERT INTO {q}.k_full VALUES (7, 'a')"),
        format!("INSERT INTO {q}.k_none VALUES (7, 'a')"),
    ] {
        pg.exec(&ddl).await;
    }
    target.create_tables(&sc, &TABLES).await;

    let cfg = client_config(&pg, &sc, &target)
        .build()
        .expect("valid config");
    let mut client = start_client(cfg, &sc.data_dir.path().join("lsn.metadata")).await;
    client
        .wait_marker_state(&sc, "completed", SMALL_SNAPSHOT_TIMEOUT)
        .await;
    for table in ["k_composite", "k_index", "k_full"] {
        pg.exec(&format!("UPDATE {q}.{table} SET v = 'b'")).await;
    }
    client
        .wait_until(
            "an update message per keyed table",
            SMALL_SNAPSHOT_TIMEOUT,
            || async {
                for table in ["k_composite", "k_index", "k_full"] {
                    if target.audit(table).await.count('u') == 0 {
                        return Some(format!("no update message of {table} yet"));
                    }
                }
                None
            },
        )
        .await;

    for (table, fields) in [
        ("k_composite", vec!["tenant", "id"]),
        ("k_index", vec!["code"]),
        ("k_full", vec!["id", "v"]),
    ] {
        let audit = target.audit(table).await;
        let snapshot = audit.of('r').next().expect("a snapshot message");
        let update = audit.of('u').next().expect("an update message");
        assert_eq!(
            key_fields(snapshot.key.as_deref().expect("a keyed snapshot message")),
            fields,
            "{table}: key columns, in the relation's order"
        );
        assert_eq!(
            snapshot.key, update.key,
            "{table}: the snapshot message and the update of the same row have one key"
        );
    }
    let none = target.audit("k_none").await;
    assert_eq!(none.0.len(), 1);
    assert_eq!(none.0[0].key, None, "no key columns, no key");
    client.stop().await.expect("clean shutdown");
}

/// `execute_snapshot_events_batch` is the delivery barrier: when it returns, the broker has every
/// message of the batch (the snapshot marks the copy complete on the strength of it). Read right
/// after the call, with no flush or close in between. A producer that lingers for a second before
/// sending its last partial batch makes a call that returns early visible.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_snapshot_batch_is_delivered_when_it_returns() {
    const ROWS: i64 = 1_000;
    let target = KafkaTarget::new().await;
    std::env::set_var("CDC_KAFKA_LINGER_MS", "1000");
    let pg = PgFixture::connect().await;
    let sc = pg.reset("delivered_kafka").await;
    target.reset().await;
    target.create_tables(&sc, &["snap_items"]).await;
    let events: Vec<ChangeEvent> = (1..=ROWS)
        .map(|id| {
            let row = RowData::from_pairs(vec![
                ("id", ColumnValue::text(&id.to_string())),
                ("name", ColumnValue::text(&format!("name-{id}"))),
                ("payload", ColumnValue::Null),
                ("note", ColumnValue::Null),
            ]);
            ChangeEvent::insert(sc.schema.as_str(), "snap_items", 1, row, Lsn(1))
        })
        .collect();

    let mut handler = target.new_handler().await;
    std::env::set_var("CDC_KAFKA_LINGER_MS", LINGER_MS.to_string());
    handler.begin_snapshot_writes();
    handler
        .execute_snapshot_events_batch(&events, Lsn(42))
        .await
        .expect("publish the batch");
    let topic = target.topic("snap_items");
    let high = target
        .blocking(move |r| r.high_watermark(&topic).expect("the topic"))
        .await;
    assert_eq!(high, ROWS, "messages on the broker when the call returned");
    handler.close().await.expect("close");
    assert_eq!(target.digest("snap_items").await.count, ROWS as u64);
}

/// Rows a crashed snapshot had published.
const PARTIAL_ROWS: i64 = 1_000;

/// S4: after a crash the leftovers cannot be cleaned (nothing can be un-published): `truncate` and
/// `fail_if_not_empty` re-run the snapshot, which publishes the already published rows again;
/// `append` refuses as for any destination.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_s4_restart_duplicates_are_allowed() {
    let begun = Instant::now();
    let target = KafkaTarget::new().await;
    let pg = PgFixture::connect().await;

    // State injection, as the SQL targets' S4 does: a slot made by SQL, an `in_progress` marker
    // naming it, and the first `PARTIAL_ROWS` snapshot messages the crashed run had delivered.
    let crashed = |name: &'static str| {
        let (pg, target) = (&pg, &target);
        async move {
            let sc = fresh_items(pg, target, name).await;
            let point = pg.create_slot(&sc.slot).await;
            write_marker(
                &sc,
                serde_json::json!({
                    "state": "in_progress",
                    "slot_name": sc.slot,
                    "consistent_point": point,
                    "tables": [[sc.schema, "snap_items"]],
                }),
            );
            target
                .publish_snapshot_rows(pg, &sc, "snap_items", 1..=PARTIAL_ROWS, point)
                .await;
            (sc, point)
        }
    };

    for (name, policy) in [
        ("s4_truncate_kafka", SnapshotTargetPolicy::Truncate),
        ("s4_fail_kafka", SnapshotTargetPolicy::FailIfNotEmpty),
    ] {
        let (sc, old_point) = crashed(name).await;
        assert_eq!(target.digest("snap_items").await.count, PARTIAL_ROWS as u64);
        let mut client = start_with(&pg, &sc, &target, policy).await;
        complete(&mut client, &pg, &target, &sc, &["snap_items"]).await;
        assert_eq!(pg.slot_count(&sc.slot).await, 1, "{name}: exactly one slot");

        let items = target.audit("snap_items").await;
        let new_point = *items
            .snapshot_points()
            .iter()
            .max()
            .expect("snapshot messages");
        assert!(new_point > old_point, "{name}: a new slot replaced the old");
        println!("{name}: {} messages {:?}", items.0.len(), items.by_op());
        assert_eq!(
            items.0.len(),
            (SMALL_ROWS + PARTIAL_ROWS) as usize,
            "{name}: every row once, the crashed run's rows twice (at-least-once)"
        );
        assert_eq!(items.distinct_snapshot_ids(), SMALL_ROWS as usize);
        assert_eq!(
            items.snapshot_points(),
            BTreeSet::from([old_point, new_point])
        );
        assert_eq!(
            items.of('r').filter(|m| m.lsn == Some(old_point)).count(),
            PARTIAL_ROWS as usize
        );
        for id in [1, 7, PARTIAL_ROWS, SMALL_ROWS] {
            assert_eq!(
                target.row("snap_items", id).await,
                pg.row(&sc, "snap_items", id).await,
                "{name}: replay of id {id} differs from PostgreSQL"
            );
        }
        client.stop().await.expect("clean shutdown");
    }

    // `append` cannot tell snapshot rows from pre-existing ones, whatever the destination.
    let (sc, point) = crashed("s4_append_kafka").await;
    let marker_before = marker_bytes(&sc);
    let mut client = start_with(&pg, &sc, &target, SnapshotTargetPolicy::Append).await;
    let msg = fail_with(&mut client, "crash recovery under append").await;
    assert_mentions(&msg, &["CDC_SNAPSHOT_TARGET_POLICY=append", "by hand"]);
    assert_eq!(
        target.audit("snap_items").await.0.len(),
        PARTIAL_ROWS as usize,
        "nothing published"
    );
    assert_eq!(pg.slot_count(&sc.slot).await, 1, "the slot is untouched");
    assert_eq!(pg.slot_confirmed_flush(&sc.slot).await, point);
    assert_eq!(marker_bytes(&sc), marker_before, "the marker is untouched");
    println!("S4 total {:?}", begun.elapsed());
}

/// S6: the slot is lost while the marker says `completed`. `fail_if_not_empty` refuses (a decision
/// that does not depend on the destination); `truncate` re-syncs, which for Kafka publishes every
/// row again and cannot remove what was published before.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_s6_resync_duplicates_are_allowed() {
    let begun = Instant::now();
    let target = KafkaTarget::new().await;
    let pg = PgFixture::connect().await;
    let sc = fresh_items(&pg, &target, "s6_kafka").await;

    let mut client = start_with(&pg, &sc, &target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    complete(&mut client, &pg, &target, &sc, &["snap_items"]).await;
    let first_slot_point = pg.slot_confirmed_flush(&sc.slot).await;
    let first_point = *target
        .audit("snap_items")
        .await
        .snapshot_points()
        .iter()
        .next()
        .expect("snapshot messages");
    let marker_before = marker_bytes(&sc);
    client.stop().await.expect("clean shutdown");

    pg.drop_slot_if_exists(&sc.slot).await;
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name) VALUES ({}, 'after-slot-loss')",
        sc.schema,
        SMALL_ROWS + 1
    ))
    .await;
    target.insert_sentinel("snap_items", SENTINEL_ID).await;

    let mut client = start_with(&pg, &sc, &target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "slot lost under fail_if_not_empty").await;
    assert_mentions(&msg, &["CDC_SNAPSHOT_TARGET_POLICY=truncate"]);
    assert_eq!(marker_bytes(&sc), marker_before, "the marker is untouched");
    assert_eq!(pg.slot_count(&sc.slot).await, 0, "no slot may be created");
    assert_eq!(
        target.audit("snap_items").await.0.len(),
        SMALL_ROWS as usize + 1,
        "nothing published: the snapshot and the sentinel"
    );

    let mut client = start_with(&pg, &sc, &target, SnapshotTargetPolicy::Truncate).await;
    // The re-sync clears the old `completed` marker; the new slot starts past the old one.
    let (pg_ref, sc_ref) = (&pg, &sc);
    client
        .wait_until("re-synced marker", SMALL_SNAPSHOT_TIMEOUT, || async move {
            let state = sc_ref.marker_state();
            let point = match pg_ref.slot_count(&sc_ref.slot).await {
                1 => pg_ref.slot_confirmed_flush(&sc_ref.slot).await,
                _ => 0,
            };
            match (state.as_deref(), point) {
                (Some("completed"), p) if p > first_slot_point => None,
                _ => Some(format!(
                    "marker {state:?}, slot at {point}, old slot point {first_slot_point}"
                )),
            }
        })
        .await;
    // Every source row, plus the sentinel that no truncate can remove.
    let expected = SMALL_ROWS as u64 + 2;
    client
        .wait_until(
            "every row published again",
            SMALL_SNAPSHOT_TIMEOUT,
            || async {
                let digest = target.digest("snap_items").await;
                (digest.count != expected).then(|| format!("{digest:?}, wanted {expected} rows"))
            },
        )
        .await;
    let items = target.audit("snap_items").await;
    let new_point = *items
        .snapshot_points()
        .iter()
        .max()
        .expect("snapshot messages");
    println!("S6: {} messages {:?}", items.0.len(), items.by_op());
    assert_eq!(
        items.0.len(),
        2 * SMALL_ROWS as usize + 2,
        "the first copy, the sentinel, the whole second copy and the row written while the slot was gone"
    );
    assert_eq!(items.distinct_snapshot_ids(), SMALL_ROWS as usize + 1);
    assert!(new_point > first_point, "the re-sync took a new snapshot");
    assert_eq!(
        items.snapshot_points(),
        BTreeSet::from([first_point, new_point])
    );
    assert!(
        target.row("snap_items", SENTINEL_ID).await.is_some(),
        "Kafka cannot truncate: the stale row stays"
    );
    assert_eq!(
        target.row("snap_items", SMALL_ROWS + 1).await,
        pg.row(&sc, "snap_items", SMALL_ROWS + 1).await,
        "the row written while the slot was gone"
    );
    assert_eq!(pg.slot_count(&sc.slot).await, 1, "exactly one slot");
    client
        .wait_for_row(
            &target,
            "snap_items",
            1,
            |row| row[1] == "name-1",
            LIVE_ROW_TIMEOUT,
        )
        .await;
    client.stop().await.expect("clean shutdown");
    println!("S6 total {:?}", begun.elapsed());
}

// ---------------------------------------------------------------------------------------------
// Memory (S11) and message size
// ---------------------------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod memory {
    use super::*;
    use snapshot_scenarios::MemoryCase;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn kafka_s11b_one_huge_row() {
        let target = KafkaTarget::new().await;
        snapshot_scenarios::s11_memory_bound(&target, MemoryCase::OneHugeRow).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn kafka_s11c_memory_independent_of_table_size() {
        let target = KafkaTarget::new().await;
        snapshot_scenarios::s11_memory_bound(&target, MemoryCase::TableSizeIndependence).await;
    }
}

/// Kafka limits a message at three places: librdkafka's `message.max.bytes` on the producer (the
/// uncompressed message; `CDC_KAFKA_MESSAGE_MAX_BYTES`), and the broker's `message.max.bytes` /
/// topic `max.message.bytes` (the compressed batch). A row whose message crosses one of them fails
/// the snapshot with `MessageSizeTooLarge`: it neither hangs nor skips the row. The marker keeps
/// demanding a cleanup, and once the limit is raised the same config completes (the rows of the
/// failed attempt are published again).
async fn oversize_row_fails_the_snapshot_then_recovers(
    tag: &str,
    producer_limit: usize,
    topic_limit: usize,
    payload_sql: &str,
    payload_bytes: usize,
) {
    const BIG_ROW_ID: i64 = 101;
    let begun = Instant::now();
    let target = KafkaTarget::with_message_max_bytes(producer_limit).await;
    target
        .topic_max_message_bytes
        .store(topic_limit, Ordering::Relaxed);
    let pg = PgFixture::connect().await;
    let sc = pg.reset(tag).await;
    target.reset().await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    TableSet::seed_items(&pg, &sc, 100).await;
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name, payload) VALUES ({BIG_ROW_ID}, 'big', {payload_sql})",
        sc.schema
    ))
    .await;

    let config = || {
        client_config(&pg, &sc, &target)
            .snapshot_target_policy(SnapshotTargetPolicy::Truncate)
            .build()
            .expect("valid config")
    };
    let lsn_file = sc.data_dir.path().join("lsn.metadata");
    let mut client = start_client(config(), &lsn_file).await;
    let err = client
        .wait_err(SMALL_SNAPSHOT_TIMEOUT)
        .await
        .expect("a row over a Kafka message limit must fail the snapshot");
    println!("{tag}: the client failed with: {err}");
    assert!(
        err.to_string().contains("MessageSizeTooLarge"),
        "the failure must name the Kafka error: {err}"
    );
    assert_eq!(
        sc.marker_state().as_deref(),
        Some("in_progress"),
        "the marker must keep demanding a cleanup"
    );
    assert!(
        target.row("snap_items", BIG_ROW_ID).await.is_none(),
        "the oversize row must not be published"
    );

    target.set_message_max_bytes(MESSAGE_MAX_BYTES);
    target
        .alter_topic_limit("snap_items", MESSAGE_MAX_BYTES)
        .await;
    let mut client = start_client(config(), &lsn_file).await;
    complete(&mut client, &pg, &target, &sc, &["snap_items"]).await;
    let copied = target.row("snap_items", BIG_ROW_ID).await.expect("big row");
    assert_eq!(copied[2].len(), 2 + 2 * payload_bytes);
    assert_eq!(
        Some(copied),
        pg.row(&sc, "snap_items", BIG_ROW_ID).await,
        "the big row"
    );
    client.stop().await.expect("clean shutdown");
    println!("{tag} total {:?}", begun.elapsed());
}

/// A normal row's message is ~1.5 KB; the big row's is ~80 KB, over a 64 KiB producer limit.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_row_over_producer_message_max_bytes_fails_the_snapshot_loudly() {
    oversize_row_fails_the_snapshot_then_recovers(
        "oversize_producer",
        64 * 1024,
        MESSAGE_MAX_BYTES,
        "decode(repeat('ab', 40000), 'hex')",
        40_000,
    )
    .await;
}

/// The same row with random bytes (incompressible, ~100 KB after lz4) against a topic that takes
/// no more than 64 KiB: the producer sends it, the broker refuses the batch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn kafka_row_over_topic_max_message_bytes_fails_the_snapshot_loudly() {
    oversize_row_fails_the_snapshot_then_recovers(
        "oversize_topic",
        MESSAGE_MAX_BYTES,
        64 * 1024,
        "(SELECT decode(string_agg(md5(g::text || random()::text), ''), 'hex') \
         FROM generate_series(1, 6400) g)",
        6_400 * 16,
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-kafka"]
async fn bench_snapshot_throughput_kafka() {
    let target = KafkaTarget::new().await;
    // The suite's 0 ms linger only drains S1's live backlog faster; the benchmark measures the
    // destination as configured by default.
    std::env::remove_var("CDC_KAFKA_LINGER_MS");
    snapshot_scenarios::bench_grid(&target).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-kafka"]
async fn bench_live_throughput_kafka() {
    let target = KafkaTarget::new().await;
    // As above: the destination's default 5 ms linger, not the suite's 0 ms.
    std::env::remove_var("CDC_KAFKA_LINGER_MS");
    snapshot_scenarios::bench_live_grid(&target).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-kafka"]
async fn bench_live_latency_kafka() {
    let target = KafkaTarget::new().await;
    std::env::remove_var("CDC_KAFKA_LINGER_MS");
    let l = snapshot_scenarios::bench_live_latency(&target, 100).await;
    println!(
        "live latency, idle pipeline, commit to visible: {} single-row transactions, min {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, max {:.1} ms",
        l.samples, l.min_ms, l.p50_ms, l.p95_ms, l.max_ms
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-kafka"]
async fn bench_live_drain_kafka() {
    let target = KafkaTarget::new().await;
    std::env::remove_var("CDC_KAFKA_LINGER_MS");
    snapshot_scenarios::bench_live_drain_grid(&target).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-kafka"]
async fn bench_snapshot_reference_kafka() {
    let target = KafkaTarget::new().await;
    std::env::remove_var("CDC_KAFKA_LINGER_MS");
    snapshot_scenarios::bench_snapshot_reference(&target).await;
}
