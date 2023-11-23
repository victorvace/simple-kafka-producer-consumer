package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        try (KafkaProducer<String, String> producer = configureKafkaProducer()) {
            String topic = "topic-test";
            String key = "testKey";
            String value = "testValue";

            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Error al enviar el registro: {}", e.getMessage());
                } else {
                    logger.info("Registro del tópico {} enviado con éxito. Offset: {}, Partition: {}",
                            recordMetadata.topic(), recordMetadata.offset(), recordMetadata.partition());
                }
            }).get();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("La ejecución del hilo fue interrumpida. Detalles del error: {}", e.getMessage());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ConfigException) {
                logger.error("Error de configuración del productor: {}", e.getCause().getMessage());
            } else {
                logger.error("Error al enviar el mensaje al servidor Kafka. Detalles del error: {}", e.getMessage());
            }
        }
    }

    private static KafkaProducer<String, String> configureKafkaProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        return new KafkaProducer<>(props);
    }
}
