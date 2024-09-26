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

public class ClusteredExecutorTaskBenchmark {

    @State(Scope.Benchmark)
    public static class TaskBenchmarkState {
        public ExecutorService executor;
        public Callable<Integer> task;

        @Setup(Level.Trial)
        public void setupExecutor() {
            executor = new ClusteredExecutor();
            task = () -> 0;
        }

        @TearDown
        public void tearDown() {
            executor.close();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int submit(TaskBenchmarkState state) throws InterruptedException, ExecutionException {
        return state.executor.submit(state.task).get();
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
            .include(ClusteredExecutorTaskBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

}
