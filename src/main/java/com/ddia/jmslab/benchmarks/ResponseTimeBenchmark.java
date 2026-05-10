package com.ddia.jmslab.benchmarks;

import com.ddia.jmslab.Consumer;
import com.ddia.jmslab.JmsMetrics;
import com.ddia.jmslab.Producer;

public class ResponseTimeBenchmark {

    public void run() {

        JmsMetrics metrics1 = new JmsMetrics();
        JmsMetrics metrics2 = new JmsMetrics();

        Producer producer = new Producer(metrics1);
        producer.start("TEST.RESPONSE");

        Consumer consumer = new Consumer(metrics2);
        consumer.start("TEST.RESPONSE");

        String payload = create1KBMessage();

        metrics1.start();

        // ---------------- producer test ----------------
        for (int i = 0; i < 1000; i++) {
            producer.sendMessage(payload);
        }
        metrics1.stop();

        metrics2.start();

        // ---------------- consumer test ----------------
        for (int i = 0; i < 1000; i++) {
            consumer.receiveMessage();
        }

        metrics2.stop();

        System.out.println("Producer Median response time: " + metrics1.getMedianResponseTimeMs() + " ms");
        System.out.println("Consumer Median response time: " + metrics2.getMedianResponseTimeMs() + " ms");

        producer.stop();
        consumer.stop();
    }

    private String create1KBMessage() {
        StringBuilder sb = new StringBuilder();

        while (sb.length() < 1024) {
            sb.append("A");
        }

        return sb.toString();
    }
}