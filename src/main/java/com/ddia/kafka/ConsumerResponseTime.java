package com.ddia.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ConsumerResponseTime {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "response-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(List.of("response-test2"));

        List<Long> times = new ArrayList<>();

        int received = 0;

        while (received < 1000) {

            long start = System.nanoTime();

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            long end = System.nanoTime();

            if (!records.isEmpty()) {

                long durationMicros = (end - start) / 1000;

                times.add(durationMicros);

                received += records.count();
            }
        }

        consumer.close();

        Collections.sort(times);

        long median = times.get(times.size() / 2);

        System.out.println("Median Consumer Response Time: "
                + median + " microseconds");
    }
}
