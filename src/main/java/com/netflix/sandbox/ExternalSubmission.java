package com.netflix.sandbox;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class ExternalSubmission {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"true", "false"})
        boolean clustered;

        public ExecutorService executor;

        @Setup(Level.Trial)
        public void setupExecutor() {
            ClusteredExecutors.Cluster cluster = ClusteredExecutors.availableClusters().getFirst();
            int parallelism = cluster.availableProcessors();
            ForkJoinPool.ForkJoinWorkerThreadFactory factory = clustered ? ClusteredExecutors.clusteredForkJoinWorkerThreadFactory(cluster) : ForkJoinPool.defaultForkJoinWorkerThreadFactory;
            executor = new ForkJoinPool(parallelism, factory, null, true);
        }
    }

    @Benchmark
    public String submit(BenchmarkState state) throws Exception {
        return state.executor.submit(() -> "Hello there.").get();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ExternalSubmission.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
