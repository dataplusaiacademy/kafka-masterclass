package com.masterclass.kafka;

public final class KafkaConfig {
    public static final String TOPIC = "orders.events";
    public static final String BOOTSTRAP_SERVERS =
            getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
    public static final String SCHEMA_REGISTRY_URL =
            getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

    private KafkaConfig() {}

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
