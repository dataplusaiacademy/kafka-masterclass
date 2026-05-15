package com.masterclass.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public final class OrderConsumer {
    public static void main(String[] args) {
        int maxMessages = parseMaxMessages(System.getenv("MAX_MESSAGES"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", KafkaConfig.SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));
            System.out.printf("Consuming from '%s'%s ...%n",
                    KafkaConfig.TOPIC,
                    maxMessages > 0 ? " (max " + maxMessages + " messages)" : " (Ctrl+C to stop)");

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            int consumed = 0;
            while (maxMessages <= 0 || consumed < maxMessages) {
                for (ConsumerRecord<String, OrderEvent> record : consumer.poll(Duration.ofSeconds(1))) {
                    OrderEvent event = record.value();
                    System.out.printf(
                            "Consumed order_id=%s status=%s customer_id=%s amount_cents=%d "
                                    + "partition=%d offset=%d%n",
                            event.getOrderId(),
                            event.getStatus(),
                            event.getCustomerId(),
                            event.getAmountCents(),
                            record.partition(),
                            record.offset());
                    consumed++;
                    if (maxMessages > 0 && consumed >= maxMessages) {
                        System.out.printf("Reached MAX_MESSAGES=%d, exiting.%n", maxMessages);
                        return;
                    }
                }
            }
        }
    }

    private static String consumerGroupId() {
        String value = System.getenv("CONSUMER_GROUP_ID");
        return value == null || value.isBlank() ? "order-fulfillment-java" : value;
    }

    private static String autoOffsetReset() {
        String value = System.getenv("AUTO_OFFSET_RESET");
        return value == null || value.isBlank() ? "earliest" : value;
    }

    private static int parseMaxMessages(String value) {
        if (value == null || value.isBlank()) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    private OrderConsumer() {}
}
