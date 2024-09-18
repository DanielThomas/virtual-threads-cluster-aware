package com.netflix.sandbox;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class VirtualThreadsSchedulerComparisonBenchmark {

    @State(Scope.Benchmark)
    public static class SchedulerBenchmarkState {
        @Param({"SINGLE", "SINGLE_WITH_AFFINITY", "CLUSTERED", "DEFAULT"})
        public Scheduler scheduler;

        public ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        
        public int numTasks = Runtime.getRuntime().availableProcessors() * 100;
        public List<Callable<Future<String>>> tasks;

        @Setup(Level.Trial)
        public void setupExecutor() {
            Class<? extends Executor> implClass = switch(scheduler) {
                case SINGLE -> SingleThreadExecutor.class;
                case SINGLE_WITH_AFFINITY -> SingleThreadWithAffinityExecutor.class;
                case CLUSTERED -> ProcessorClusteredExecutorService.class;
                default -> null;
            };
            if (implClass != null) {
                System.setProperty("jdk.virtualThreadScheduler.implClass", implClass.getCanonicalName());
            }
            Faker faker = new Faker();
            tasks = IntStream.range(0, numTasks).mapToObj(_ -> (Callable<Future<String>>) () -> {
                Address address = faker.address();
                NameAndCity request = new NameAndCity(address.firstName(), address.lastName(), address.cityName());
                return executor.submit(() -> "Hello " + request.firstName + " from " + request.cityName);
            }).toList();
        }
    }

    @Benchmark
    public String submit(SchedulerBenchmarkState state) throws ExecutionException, InterruptedException {
        return state.executor.submit(state.tasks.getFirst()).get().get();
    }

    //@Benchmark
    public List<String> invokeAll(SchedulerBenchmarkState state) throws InterruptedException {
        return state.executor.invokeAll(state.tasks).stream().map(future -> {
            try {
                return future.get().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    private record NameAndCity(String firstName, String lastName, String cityName) {
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(VirtualThreadsSchedulerComparisonBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    public enum Scheduler {
        SINGLE,
        SINGLE_WITH_AFFINITY,
        DEFAULT,
        CLUSTERED
    }

}
