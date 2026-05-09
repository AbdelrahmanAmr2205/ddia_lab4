package com.jmslab.benchmarks;

import com.jmslab.Consumer;
import com.jmslab.JmsMetrics;
import com.jmslab.Producer;

public class LatencyBenchmark {

    public void run() {

        JmsMetrics metrics = new JmsMetrics();

        Producer producer = new Producer(metrics);
        Consumer consumer = new Consumer(metrics);

        producer.start("TEST.LATENCY");
        consumer.start("TEST.LATENCY");

        String payload = create1KBMessage();

        metrics.start();

        int N = 10000;

        try {
            for (int i = 0; i < N; i++) {
                producer.sendMessage(payload);
                consumer.receiveMessage();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        metrics.stop();

        System.out.println("Median Latency: " + metrics.getMedianLatencyMs() + " ms");
        System.out.println("Avg Latency: " + metrics.getAverageLatencyMs() + " ms");
        
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