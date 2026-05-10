package com.ddia.jmslab;

import com.ddia.jmslab.benchmarks.LatencyBenchmark;
import com.ddia.jmslab.benchmarks.ResponseTimeBenchmark;
import com.ddia.jmslab.benchmarks.ThroughputBenchmark;

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

