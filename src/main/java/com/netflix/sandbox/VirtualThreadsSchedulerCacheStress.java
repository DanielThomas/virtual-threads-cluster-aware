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
        @Param({"WORK_STEALING", "DEFAULT"})
        public Scheduler scheduler;

        @Param({"8"})
        public int numTasks;

        public ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        public int[][] data;

        public List<Callable<List<Integer>>> tasks;

        @Setup(Level.Trial)
        public void setupExecutor() {
            Class<? extends Executor> implClass = switch (scheduler) {
                case WORK_STEALING -> WorkStealingExecutor.class;
                case DEFAULT -> null;
                default -> throw new IllegalArgumentException("missing implClass condition for " + scheduler);
            };
            if (implClass != null) {
                System.setProperty("jdk.virtualThreadScheduler.implClass", implClass.getCanonicalName());
            }
            List<ClusteredExecutors.Cluster> clusters = ClusteredExecutors.availableClusters();
            if (numTasks == 0) {
                numTasks = clusters.getFirst().availableProcessors();
            }
            int numClusters = clusters.size();
            data = new int[numClusters][];
            tasks = IntStream.range(0, 1).mapToObj(i -> {
                ClusteredExecutors.Cluster cluster = clusters.get(i);
                data[i] = ThreadLocalRandom.current().ints(cluster.lastLevelCacheSize().getAsLong()).toArray();
                return (Callable<List<Integer>>) () -> {
                    List<Future<Integer>> futures = IntStream.range(0, numTasks)
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
        return state.executor.invokeAll(state.tasks).stream().flatMap(future -> {
            try {
                return future.get().stream();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).toList();
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
        WORK_STEALING
    }

}
