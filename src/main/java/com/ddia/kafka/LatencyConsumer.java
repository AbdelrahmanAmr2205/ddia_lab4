package com.ddia.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class LatencyConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "latency-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(List.of("latency-test"));

        List<Long> latencies = new ArrayList<>();

        while (latencies.size() < 10000) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            long now = System.currentTimeMillis();

            for (ConsumerRecord<String, String> record : records) {

                long sent =
                        Long.parseLong(record.value());

                long latency = now - sent;

                latencies.add(latency);
            }
        }

        consumer.close();

        Collections.sort(latencies);

        long median =
                latencies.get(latencies.size() / 2);

        System.out.println("Median E2E Latency: "
                + median + " ms");
    }
}
