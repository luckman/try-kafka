package com.luckman.examples.try_kafka.producer1;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerSample1 {
    private static final String TOPIC_NAME = "sample_topic";
    private static final int MESSAGES_AMOUNT = 5;
    private static final String PROPERTIES = "local.kafka.properties";

    public static void main(String[] args) throws Exception {
        final Properties props = loadConfig(PROPERTIES);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGES_AMOUNT; i++) {
                String key = "key " + i;
                String value = "value " + i;
                System.out.printf("Producing record: %s\t%s%n", key, value);
                producer.send(new ProducerRecord<>(TOPIC_NAME, key, value), (m, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }
                });
                producer.flush();

                Thread.sleep(5000);
            }

            System.out.printf(MESSAGES_AMOUNT + " messages were produced to topic %s%n", TOPIC_NAME);
        }

    }

    public static Properties loadConfig(String configFile) throws IOException {
        final Properties cfg = new Properties();
        try (InputStream inputStream = ProducerSample1.class.getClassLoader().getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

}
