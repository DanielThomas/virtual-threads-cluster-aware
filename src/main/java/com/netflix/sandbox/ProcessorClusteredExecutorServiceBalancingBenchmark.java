package com.netflix.sandbox;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class ProcessorClusteredExecutorServiceBalancingBenchmark {

    @State(Scope.Benchmark)
    public static class BalancingBenchmarkState {
        public ProcessorClusteredExecutorService executor = new ProcessorClusteredExecutorService();

        @Setup(Level.Trial)
        public void setupExecutor() {
            IntStream.range(0, Runtime.getRuntime().availableProcessors() * 10_000)
                .forEach(_ -> executor.submit(() -> {
                    try {
                        Thread.sleep(Duration.ofDays(1));
                    } catch (InterruptedException _) {
                    }
                }));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public ForkJoinPool chooseLeastLoadedPool(BalancingBenchmarkState state) {
        int[] indexes = IntStream.range(0, state.executor.pools().size()).toArray();
        ForkJoinPool preferredPool = state.executor.pools().getFirst();
        return state.executor.chooseLeastLoadedPool(preferredPool, indexes, ProcessorClusteredExecutorService.DEFAULT_LOAD_FUNCTION);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(ProcessorClusteredExecutorServiceBalancingBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

}
