package com.jmslab;

import com.jmslab.benchmarks.LatencyBenchmark;
import com.jmslab.benchmarks.ResponseTimeBenchmark;
import com.jmslab.benchmarks.ThroughputBenchmark;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {

        System.out.println("=== JMS BENCHMARK START ===");

        System.out.println("=== RESPONSE TIME ===");
        new ResponseTimeBenchmark().run();

        System.out.println("=== THROUGHPUT ===");
        new ThroughputBenchmark().run();

        System.out.println("=== LATENCY ===");
        new LatencyBenchmark().run();

        System.out.println("=== DONE ===");
    }
}

