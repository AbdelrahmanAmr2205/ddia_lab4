package com.jmslab.benchmarks;

import com.jmslab.JmsMetrics;
import com.jmslab.Producer;
import com.jmslab.Consumer;

public class ThroughputBenchmark {

    public void run() {

        JmsMetrics metrics1 = new JmsMetrics();
        JmsMetrics metrics2 = new JmsMetrics();

        Producer producer = new Producer(metrics1);
        producer.start("TEST.THROUGHPUT");

        Consumer consumer = new Consumer(metrics2);
        consumer.start("TEST.THROUGHPUT");

        int X = 5000;

        String payload = create1KBMessage();

        metrics1.start();

        // ---------------- PRODUCE ----------------
        for (int i = 0; i < X; i++) {
            producer.sendMessage(payload);
        }
        metrics1.stop();


        metrics2.start();
        // ---------------- CONSUME ----------------
        for (int i = 0; i < X; i++) {
            consumer.receiveMessage();
        }

        metrics2.stop();

        System.out.println("Messages: " + X);
        System.out.println("Producer Throughput: " + metrics1.getThroughput() + " msg/sec");
        System.out.println("Consumer Throughput: " + metrics2.getThroughput() + " msg/sec");

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