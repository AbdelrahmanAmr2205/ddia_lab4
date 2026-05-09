package com.jmslab;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JmsMetrics {

    // Timing window
    private long startTimeNs;
    private long endTimeNs;

    // Counters
    private int messageCount = 0;

    // Samples (IMPORTANT)
    private List<Long> responseTimesNs = new ArrayList<>();
    private List<Long> latencySamplesNs = new ArrayList<>();

    // Lifecycle
    public void start() {
        startTimeNs = System.nanoTime();
    }

    public void stop() {
        endTimeNs = System.nanoTime();
    }

    // Response Time (Producer/Consumer API call time)
    public void recordResponseTime(long durationNs) {
        responseTimesNs.add(durationNs);
    }

    // End-to-End Latency
    public void recordLatency(long latencyNs) {
        latencySamplesNs.add(latencyNs);
    }

    // Throughput
    public double getThroughput() {
        double seconds = (endTimeNs - startTimeNs) / 1_000_000_000.0;
        if (seconds == 0) return 0;
        return messageCount / seconds;
    }

    public void incrementMessageCount() {
        messageCount++;
    }

    // Average latency
    public double getAverageLatencyMs() {
        if (latencySamplesNs.isEmpty()) return 0;

        long sum = 0;
        for (long v : latencySamplesNs) {
            sum += v;
        }

        return (sum / latencySamplesNs.size()) / 1_000_000.0;
    }

    // Median helper
    private double medianMs(List<Long> data) {
        if (data.isEmpty()) return 0;

        Collections.sort(data);

        int mid = data.size() / 2;

        long medianNs;

        if (data.size() % 2 == 0) {
            medianNs = (data.get(mid - 1) + data.get(mid)) / 2;
        } else {
            medianNs = data.get(mid);
        }

        return medianNs / 1_000_000.0;
    }

    // REQUIRED METRICS
    public double getMedianLatencyMs() {
        return medianMs(latencySamplesNs);
    }

    public double getMedianResponseTimeMs() {
        return medianMs(responseTimesNs);
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void resetMessageCount() {
        messageCount = 0;
        responseTimesNs.clear();
        latencySamplesNs.clear();
    }

    // Output
    public void printResults() {

        System.out.println("Messages: " + messageCount);

        System.out.println("Throughput: " + getThroughput() + " msg/sec");

        System.out.println("Avg latency: " + getAverageLatencyMs() + " ms");

        System.out.println("Median latency: " + getMedianLatencyMs() + " ms");

        System.out.println("Median response time: " + getMedianResponseTimeMs() + " ms");
    }
}