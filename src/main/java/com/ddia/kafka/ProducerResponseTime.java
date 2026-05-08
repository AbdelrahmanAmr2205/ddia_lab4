package com.ddia.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class ProducerResponseTime {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        List<Long> times = new ArrayList<>();

        String message = "x".repeat(1024);

        for (int i = 0; i < 1000; i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("response-test2", message);

            long start = System.nanoTime();

            producer.send(record).get();

            long end = System.nanoTime();

            long durationMicros = (end - start) / 1000;

            times.add(durationMicros);
        }

        producer.close();

        Collections.sort(times);

        long median = times.get(times.size() / 2);

        System.out.println("Median Producer Response Time: "
                + median + " microseconds");
    }
}