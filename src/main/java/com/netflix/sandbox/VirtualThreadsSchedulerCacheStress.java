package com.netflix.sandbox;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

@OutputTimeUnit(TimeUnit.MINUTES)
public class VirtualThreadsSchedulerCacheStress {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"CLUSTERED_FJP", "DEFAULT"})
        public Scheduler scheduler;

        public ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        public int[][] data;

        public List<Callable<List<Integer>>> tasks;

        @Setup(Level.Trial)
        public void setupExecutor() {
            Class<? extends Executor> implClass = switch (scheduler) {
                case CLUSTERED_FJP -> ClusteredForkJoinPool.class;
                case TIERED -> TieredExecutor.class;
                case DEFAULT -> null;
                default -> throw new IllegalArgumentException("missing implClass condition for " + scheduler);
            };
            if (implClass != null) {
                System.setProperty("jdk.virtualThreadScheduler.implClass", implClass.getCanonicalName());
            }
            int numClusters = ClusteredExecutors.availableClusters();
            data = new int[numClusters][];
            int cacheSize = LinuxScheduling.cacheSizeInBytes(0, 3);
            tasks = IntStream.range(0, numClusters).mapToObj(i -> {
                data[i] = ThreadLocalRandom.current().ints(cacheSize).toArray();
                return (Callable<List<Integer>>) () -> {
                    List<Future<Integer>> futures = IntStream.range(0, Runtime.getRuntime().availableProcessors() / numClusters)
                        .mapToObj(_ -> executor.submit(() -> Arrays.hashCode(data[i])))
                        .toList();
                    return futures.stream()
                        .map(future -> {
                            try {
                                return future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }).toList();
                };
            }).toList();
        }
    }

    @Benchmark
    public List<Integer> submit(BenchmarkState state) throws Exception {
        List<Integer> result = state.executor.invokeAll(state.tasks).stream().flatMap(future -> {
            try {
                return future.get().stream();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        return result;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(VirtualThreadsSchedulerCacheStress.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    public enum Scheduler {
        DEFAULT,
        TIERED,
        CLUSTERED_FJP
    }

}
