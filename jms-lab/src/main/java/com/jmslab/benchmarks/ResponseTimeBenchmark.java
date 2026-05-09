package com.jmslab.benchmarks;

import com.jmslab.Consumer;
import com.jmslab.JmsMetrics;
import com.jmslab.Producer;

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
        try {
            for (int i = 0; i < 1000; i++) {
                producer.sendMessage(payload);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        metrics1.stop();

        metrics2.start();

        // ---------------- consumer test ----------------
        try {
            for (int i = 0; i < 1000; i++) {
                consumer.receiveMessage();
            }
        } catch (Exception e) {
            e.printStackTrace();
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