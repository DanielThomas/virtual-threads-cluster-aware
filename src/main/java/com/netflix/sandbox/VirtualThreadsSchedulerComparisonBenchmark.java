package com.netflix.sandbox;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import org.openjdk.jmh.annotations.*;
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
        @Param({"CLUSTERED", "DEFAULT"})
        public Scheduler scheduler;

        public ExecutorService executor;
        
        public int numTasks = Runtime.getRuntime().availableProcessors() * 100;
        public List<Callable<Future<String>>> tasks;

        @Setup(Level.Trial)
        public void setupExecutor() {
            executor = switch(scheduler) {
                case DEFAULT ->  Executors.newVirtualThreadPerTaskExecutor();
                case CLUSTERED -> {
                    System.setProperty("jdk.virtualThreadScheduler.implClass", ProcessorClusteredExecutorService.class.getCanonicalName());
                    yield Executors.newVirtualThreadPerTaskExecutor();
                }
            };
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
        DEFAULT,
        CLUSTERED
    }

}
