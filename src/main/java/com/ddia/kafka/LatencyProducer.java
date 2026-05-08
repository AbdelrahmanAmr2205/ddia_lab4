package com.ddia.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class LatencyProducer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        for (int i = 0; i < 10000; i++) {

            long timestamp = System.currentTimeMillis();

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "latency-test",
                            String.valueOf(timestamp)
                    );

            producer.send(record);

            Thread.sleep(1);
        }

        producer.flush();
        producer.close();

        System.out.println("Finished producing.");
    }
}
