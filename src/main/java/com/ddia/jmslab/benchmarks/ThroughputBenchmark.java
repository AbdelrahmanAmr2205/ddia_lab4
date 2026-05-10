package com.ddia.jmslab.benchmarks;

import com.ddia.jmslab.JmsMetrics;
import com.ddia.jmslab.Producer;
import com.ddia.jmslab.Consumer;

public class ThroughputBenchmark {

    public void run() {
        System.out.println("=== Starting Throughput Benchmark (Exponential Increase) ===");
        String payload = create1KBMessage();

        int targetX = 100;
        int loops = 20;
        int maxSuccessfulX = 0;

        while (true) {
            System.out.println("Testing Target Throughput: " + targetX + " msg/sec...");

            JmsMetrics prodMetrics = new JmsMetrics();
            JmsMetrics consMetrics = new JmsMetrics();

            Producer producer = new Producer(prodMetrics);
            producer.start("TEST.THROUGHPUT");

            Consumer consumer = new Consumer(consMetrics);
            consumer.start("TEST.THROUGHPUT");

            // Each message gets exactly this many nanoseconds of the 1-second window
            long slotNanos = 1_000_000_000L / targetX;

            // The absolute deadline: we must finish all sends within 1 second
            long startTime = System.nanoTime();
            long testDeadlineNanos = startTime + 1_050_000_000L * loops;

            prodMetrics.start();
            int sent = 0;

            for (int i = 0; i < targetX * loops; i++) {
                long slotStart = System.nanoTime();

                // KEY CHECK: if we've already blown the 1-second budget, stop early → FAIL
                if (slotStart >= testDeadlineNanos) {
                    System.out.println("   -> Deadline exceeded after " + sent + " messages");
                    break;
                }

                producer.sendMessage(payload);
                sent++;

                // Busy-wait until the END of this time slot.
                // This correctly absorbs sendMessage() duration:
                //   actual_wait = slotNanos - time_send_took
                // If sendMessage() alone exceeds slotNanos, we skip the wait
                // and the next iteration's deadline check will catch the overrun.
                long nextSlotStart = slotStart + slotNanos;
                while (System.nanoTime() < nextSlotStart) {
                    // spin
                }
            }

            prodMetrics.stop();

            long actualDurationMs = (System.nanoTime() - startTime) / 1_000_000;
            System.out.println("   -> Production phase completed in " + actualDurationMs + " ms (budget: 10000 ms)");
            System.out.println("   -> Sent: " + sent + "/" + targetX * loops);

            // Only consume what was actually sent
            consMetrics.start();
            for (int i = 0; i < sent; i++) {
                consumer.receiveMessage();
            }
            consMetrics.stop();

            producer.stop();
            consumer.stop();

            int produced = prodMetrics.getMessageCount();
            int consumed = consMetrics.getMessageCount();

            // FAIL if: we ran out of time (sent < targetX) OR broker dropped messages
            if (sent < targetX * loops || produced < targetX * loops || consumed < targetX * loops) {
                System.out.println("Test FAILED at " + targetX + " msg/sec.");
                System.out.println("  Sent in window: " + sent + "/" + targetX * loops
                        + " | Produced: " + produced + " | Consumed: " + consumed);
                break;
            }

            System.out.println("Test PASSED at " + targetX + " msg/sec.");
            maxSuccessfulX = targetX;
            targetX *= 2;
        }

        System.out.println("\n=== MAX THROUGHPUT RESULTS ===");
        System.out.println("Maximum Successful Throughput: " + maxSuccessfulX + " msg/sec");
    }

    private String create1KBMessage() {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 1024) {
            sb.append("A");
        }
        return sb.toString();
    }
}