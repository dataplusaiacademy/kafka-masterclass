package com.masterclass.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public final class OrderProducer {
    private static final List<String> STATUSES = List.of("PLACED", "PAID", "SHIPPED");

    public static void main(String[] args) throws Exception {
        int orderCount = args.length > 0 ? Integer.parseInt(args[0]) : 3;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", KafkaConfig.SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props)) {
            System.out.printf("Publishing %d order(s) to topic '%s' ...%n", orderCount, KafkaConfig.TOPIC);

            for (int i = 0; i < orderCount; i++) {
                String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);
                String customerId = "cust-" + (1000 + i);
                long amountCents = 4999L + (i * 1500L);

                for (String status : STATUSES) {
                    OrderEvent event = OrderEvent.newBuilder()
                            .setOrderId(orderId)
                            .setCustomerId(customerId)
                            .setStatus(status)
                            .setAmountCents(amountCents)
                            .setCurrency("USD")
                            .setEventTime(Instant.now().toEpochMilli())
                            .build();

                    ProducerRecord<String, OrderEvent> record =
                            new ProducerRecord<>(KafkaConfig.TOPIC, orderId, event);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("Delivery failed: %s%n", exception.getMessage());
                            return;
                        }
                        System.out.printf(
                                "Produced order_id=%s status=%s partition=%d offset=%d%n",
                                orderId, status, metadata.partition(), metadata.offset());
                    }).get();
                    Thread.sleep(100);
                }
            }
        }

        System.out.println("Producer finished.");
    }

    private OrderProducer() {}
}
