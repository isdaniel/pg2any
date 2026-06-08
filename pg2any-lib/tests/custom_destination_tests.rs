//! Tests for the pluggable destination registry on `Config`.

use async_trait::async_trait;
use pg2any_lib::destinations::{DestinationHandler, PreCommitHook};
use pg2any_lib::{Config, ConfigBuilder, DestinationType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Recorder = Arc<Mutex<Vec<String>>>;

struct MockHandler {
    calls: Recorder,
}

impl MockHandler {
    fn new(calls: Recorder) -> Self {
        Self { calls }
    }

    fn record(&self, s: impl Into<String>) {
        self.calls.lock().unwrap().push(s.into());
    }
}

#[async_trait]
impl DestinationHandler for MockHandler {
    async fn connect(&mut self, _connection_string: &str) -> pg2any_lib::CdcResult<()> {
        self.record("connect");
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> pg2any_lib::CdcResult<()> {
        self.record(format!("execute_sql_batch_with_hook:{}", commands.len()));
        if let Some(hook) = pre_commit_hook {
            hook().await?;
            self.record("hook_invoked");
        }
        Ok(())
    }

    async fn close(&mut self) -> pg2any_lib::CdcResult<()> {
        self.record("close");
        Ok(())
    }
}

fn base_builder() -> ConfigBuilder {
    Config::builder()
        .source_connection_string("postgres://localhost/src")
        .destination_connection_string("custom://nowhere")
}

#[tokio::test]
async fn test_custom_destination_via_registry() {
    let calls: Recorder = Arc::new(Mutex::new(Vec::new()));
    let calls_for_factory = calls.clone();

    let config = base_builder()
        .custom_destination(move || MockHandler::new(calls_for_factory.clone()))
        .build()
        .expect("config builds");

    assert!(matches!(
        config.destination_type,
        DestinationType::Custom(_)
    ));

    let mut handler = config
        .create_destination()
        .expect("factory returns handler");

    handler.connect("custom://nowhere").await.unwrap();

    let hook_flag: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let hook_flag_inner = hook_flag.clone();
    let hook: PreCommitHook = Box::new(move || {
        let flag = hook_flag_inner.clone();
        Box::pin(async move {
            *flag.lock().unwrap() = true;
            Ok(())
        })
    });

    handler
        .execute_sql_batch_with_hook(&["INSERT INTO t VALUES (1)".to_string()], Some(hook))
        .await
        .unwrap();

    handler.close().await.unwrap();

    assert!(*hook_flag.lock().unwrap(), "pre_commit_hook ran");

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec![
            "connect".to_string(),
            "execute_sql_batch_with_hook:1".to_string(),
            "hook_invoked".to_string(),
            "close".to_string(),
        ]
    );
}

#[tokio::test]
async fn test_builtin_destinations_via_registry() {
    #[cfg(feature = "mysql")]
    {
        let cfg = base_builder()
            .destination_type(DestinationType::MySQL)
            .build()
            .unwrap();
        assert!(cfg.create_destination().is_ok());
    }
    #[cfg(feature = "sqlserver")]
    {
        let cfg = base_builder()
            .destination_type(DestinationType::SqlServer)
            .build()
            .unwrap();
        assert!(cfg.create_destination().is_ok());
    }
    #[cfg(feature = "sqlite")]
    {
        let cfg = base_builder()
            .destination_type(DestinationType::SQLite)
            .build()
            .unwrap();
        assert!(cfg.create_destination().is_ok());
    }
    #[cfg(feature = "kafka")]
    {
        let cfg = base_builder()
            .destination_type(DestinationType::Kafka)
            .build()
            .unwrap();
        assert!(cfg.create_destination().is_ok());
    }
}

#[tokio::test]
async fn test_custom_variant_missing_registration_errors() {
    let cfg = base_builder()
        .destination_type(DestinationType::Custom("not-registered".to_string()))
        .build()
        .unwrap();

    let err = match cfg.create_destination() {
        Ok(_) => panic!("expected create_destination to fail for unregistered key"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("not-registered"),
        "error mentions missing key: {msg}"
    );
}

#[tokio::test]
async fn test_config_clone_preserves_registry() {
    let calls: Recorder = Arc::new(Mutex::new(Vec::new()));
    let calls_for_factory = calls.clone();

    let config = base_builder()
        .custom_destination(move || MockHandler::new(calls_for_factory.clone()))
        .build()
        .unwrap();

    let cloned = config.clone();
    let mut handler = cloned.create_destination().expect("clone has registry");
    handler.connect("ignored").await.unwrap();

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded, vec!["connect".to_string()]);
}

#[derive(Default)]
struct DefaultableHandler;

#[async_trait]
impl DestinationHandler for DefaultableHandler {
    async fn connect(&mut self, _connection_string: &str) -> pg2any_lib::CdcResult<()> {
        Ok(())
    }
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}
    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> pg2any_lib::CdcResult<()> {
        Ok(())
    }
    async fn close(&mut self) -> pg2any_lib::CdcResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_use_destination_default_shortcut() {
    let config = base_builder()
        .use_destination::<DefaultableHandler>()
        .build()
        .expect("config builds");

    assert!(matches!(
        config.destination_type,
        DestinationType::Custom(_)
    ));

    let mut handler = config
        .create_destination()
        .expect("use_destination registers a factory");
    handler.connect("ignored").await.unwrap();
    handler.close().await.unwrap();
}

#[tokio::test]
async fn test_factory_produces_fresh_handler_per_call() {
    // The factory must return a new handler each call: the consumer
    // constructs its own handler in addition to the main pipeline.
    let calls: Recorder = Arc::new(Mutex::new(Vec::new()));
    let calls_for_factory = calls.clone();
    let invocations = Arc::new(Mutex::new(0usize));
    let invocations_inner = invocations.clone();

    let config = base_builder()
        .custom_destination(move || {
            *invocations_inner.lock().unwrap() += 1;
            MockHandler::new(calls_for_factory.clone())
        })
        .build()
        .unwrap();

    let mut h1 = config.create_destination().unwrap();
    let mut h2 = config.create_destination().unwrap();

    h1.connect("a").await.unwrap();
    h2.connect("b").await.unwrap();

    assert_eq!(
        *invocations.lock().unwrap(),
        2,
        "factory called per handler"
    );
    let recorded = calls.lock().unwrap().clone();
    // Both handlers share the same recorder — two connects observed.
    assert_eq!(recorded, vec!["connect".to_string(), "connect".to_string()]);
}
