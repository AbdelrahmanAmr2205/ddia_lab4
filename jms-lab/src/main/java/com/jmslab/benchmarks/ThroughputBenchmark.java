package com.jmslab.benchmarks;

import com.jmslab.JmsMetrics;
import com.jmslab.Producer;
import com.jmslab.Consumer;

public class ThroughputBenchmark {

    JmsMetrics metrics1 = new JmsMetrics();
    JmsMetrics metrics2 = new JmsMetrics();

    Producer producer = new Producer(metrics1);
    Consumer consumer = new Consumer(metrics2);
    String payload = create1KBMessage();

    public void run() {

        try {
            producer.start("TEST.THROUGHPUT");
            consumer.start("TEST.THROUGHPUT");
        } catch (Exception e) {
            // TODO: handle exception
        }

        int numOfRequests = 1000;
        // int X = 5000;
        // int maxThroughput = X;
        // float T = 1/X;
        // float sleepTime = T - 0.2 * T; // 20% less than T to account for processing time

        metrics1.start();

        // // ---------------- PRODUCE ----------------
        // for (int i = 0; i < X; i++) {
        //     producer.sendMessage(payload);
        // }
        // metrics1.stop();


        // metrics2.start();
        // // ---------------- CONSUME ----------------
        // for (int i = 0; i < X; i++) {
        //     consumer.receiveMessage();
        // }

        metrics2.stop();

        System.out.println("Messages: " + numOfRequests);
        System.out.println("Producer Throughput: " + findMaxProducerThroughput(numOfRequests) + " msg/sec");
        System.out.println("Consumer Throughput: " + findMaxConsumerThroughput(numOfRequests) + " msg/sec");

        producer.stop();
        consumer.stop();
    }

    
    private long findMaxProducerThroughput(int numOfRequests) {
        long X = 100;
        long maxThroughput = X;

        while (true) {
            // Reset counters for this round
            metrics2.resetMessageCount();

            double T_ns = 1_000_000_000.0 / X; // period in nanoseconds
            long sleepNanos = (long) (T_ns - 0.2 * T_ns);

            // --- PRODUCE X messages in ~1 second ---
            // long produceStart = System.currentTimeMillis();
            for (int i = 0; i < X; i++) {
                try {
                    producer.sendMessage(payload);
                    if (sleepNanos > 0) sleepNanos(sleepNanos);
                } catch (Exception e) {
                    break;
                }
            }
            // long produceElapsed = System.currentTimeMillis() - produceStart;

            // --- CONSUME: drain all X messages ---
            for (int i = 0; i < X; i++) {
                try {
                    consumer.receiveMessage(); // has 1000ms timeout built in
                } catch (Exception e) {
                    break;
                }
            }

            int consumed = metrics2.getMessageCount();
            // boolean finishedInTime = produceElapsed <= 1500; // some tolerance
            boolean allDelivered = (consumed == X);

            if (allDelivered) {
                maxThroughput = X;
                X *= 2;
                System.out.println("Success at " + maxThroughput + " req/s. Testing " + X + "...");
            } else {
                System.out.println("Failed at " + X + " req/s. consumed=" + consumed + "/" + X);
                break;
            }
        }

        return maxThroughput;
    }

    private void sleepNanos(long nanos) {
        if (nanos <= 0) return;
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos) {
            // busy wait
        }
    }

    private int findMaxConsumerThroughput(int numOfRequests) {
        int X = 1000; // initial guess for max throughput
        int maxThroughput = X;
        boolean testPassed = true;
        while (testPassed) {
            int failedRequests = 0;
            double T = 1000.0/maxThroughput; // peiod in milliseconds
            long sleepTime = (long) (T - 0.2 * T); // 20% less than T to account for processing time

            for (int i = 0; i < numOfRequests; i++) {
                try {
                    consumer.receiveMessage();
                    Thread.sleep(sleepTime);

                } catch (Exception e) {
                    failedRequests++;
                    break; // Exit the loop early if a failure occurs
                }

            }
            // A failed test is denoted by any non-zero number of failed requests
            if (failedRequests == 0) {
                maxThroughput = X;
                X *= 2; // Exponential increase
                System.out.println("Success at " + maxThroughput + " req/s. Testing " + X + "...");
            } else {
                testPassed = false;
                System.out.println("Failed at " + X + " req/s.");
            }
        }
        
        return maxThroughput;
        }


    private String create1KBMessage() {

        StringBuilder sb = new StringBuilder();

        while (sb.length() < 1024) {
            sb.append("A");
        }

        return sb.toString();
    }
}