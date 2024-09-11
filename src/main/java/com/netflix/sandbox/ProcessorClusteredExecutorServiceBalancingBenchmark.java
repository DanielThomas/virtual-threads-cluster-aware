package com.netflix.sandbox;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class ProcessorClusteredExecutorServiceBalancingBenchmark {

    @State(Scope.Benchmark)
    public static class BalancingBenchmarkState {
        public ExecutorService executor;
        public List<Callable<String>> tasks;

        @Setup(Level.Trial)
        public void setupExecutor() {
            executor = new ProcessorClusteredExecutorService();
            long tokens = 1_000_000;
            tasks = IntStream.range(0, Runtime.getRuntime().availableProcessors() * 1000)
                .mapToObj(i -> (Callable<String>) () -> {
                    if (i % 2 == 0) {
                        Blackhole.consumeCPU(tokens * 2);
                    } else {
                        Blackhole.consumeCPU(tokens);
                    }
                    return "Hello there.";
                }).toList();
        }

        @TearDown
        public void tearDown() {
            executor.close();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public List<String> external(BalancingBenchmarkState state) throws InterruptedException {
        return state.executor.invokeAll(state.tasks).stream()
            .map(ProcessorClusteredExecutorServiceBalancingBenchmark::getUnchecked)
            .toList();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public List<String> internal(BalancingBenchmarkState state) throws InterruptedException, ExecutionException {
        return state.executor.submit(() -> state.tasks.stream().map(task -> state.executor.submit(task)).toList()).get().stream()
            .map(ProcessorClusteredExecutorServiceBalancingBenchmark::getUnchecked)
            .toList();
    }

    private static <T> T getUnchecked(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
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
