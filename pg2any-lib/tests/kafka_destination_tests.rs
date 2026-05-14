#[cfg(feature = "kafka")]
mod kafka_tests {
    use pg2any_lib::destinations::destination_factory::DestinationHandler;
    use pg2any_lib::destinations::kafka::KafkaDestination;
    use pg2any_lib::types::DestinationType;
    use pg2any_lib::DestinationFactory;

    #[test]
    fn test_destination_factory_creates_kafka() {
        let result = DestinationFactory::create(&DestinationType::Kafka);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_supports_event_mode() {
        let dest = KafkaDestination::new();
        assert!(dest.supports_event_mode());
    }

    #[tokio::test]
    async fn test_kafka_rejects_sql_batch() {
        let mut dest = KafkaDestination::new();
        let commands = vec!["INSERT INTO t VALUES (1);".to_string()];
        let result = dest.execute_sql_batch_with_hook(&commands, None).await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("does not support SQL batch"));
    }

    #[tokio::test]
    async fn test_execute_events_empty_batch() {
        let mut dest = KafkaDestination::new();
        let events: Vec<pg_walstream::ChangeEvent> = vec![];
        let ts = chrono::Utc::now();
        let result = dest
            .execute_events_batch_with_hook(&events, 1, ts, None, None)
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_destination_type_kafka_display() {
        assert_eq!(DestinationType::Kafka.to_string(), "kafka");
    }

    #[test]
    fn test_destination_type_kafka_serialization() {
        let kafka_type = DestinationType::Kafka;
        let json = serde_json::to_string(&kafka_type).unwrap();
        assert_eq!(json, "\"Kafka\"");

        let deserialized: DestinationType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, kafka_type);
    }
}
