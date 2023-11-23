package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {

        try (KafkaConsumer<String, String> consumer = configureKafkaConsumer()) {
            consumer.subscribe(Collections.singletonList("topic-test"));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    logger.info("Topic: {}", consumerRecord.topic());
                    logger.info("Partition: {}", consumerRecord.partition());
                    logger.info("Key: {}", consumerRecord.key());
                    logger.info("Value: {}", consumerRecord.value());
                }
            }
        } catch (Exception e) {
            logger.error("Error al consumir mensajes del servidor Kafka. Detalles del error: {}", e.getMessage());
        }
    }

    private static KafkaConsumer<String, String> configureKafkaConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1800");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");

        return new KafkaConsumer<>(props);
    }

}
