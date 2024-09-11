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
    public static class ExecutorState {
        @Param({"CLUSTERED", "DEFAULT"})
        public Scheduler scheduler;

        public ExecutorService executor;
        
        public int numTasks = Runtime.getRuntime().availableProcessors();
        
        public Faker faker = new Faker();

        @Setup(Level.Trial)
        public void setupExecutor() {
            executor = switch(scheduler) {
                case DEFAULT ->  Executors.newVirtualThreadPerTaskExecutor();
                case CLUSTERED -> {
                    System.setProperty("jdk.virtualThreadScheduler.implClass", ProcessorClusteredExecutorService.class.getCanonicalName());
                    yield Executors.newVirtualThreadPerTaskExecutor();
                }
            };
        }
    }

    @Benchmark
    public List<String> benchmark(ExecutorState state) throws InterruptedException {
        List<Callable<Future<String>>> tasks = IntStream.range(0, state.numTasks).mapToObj(_ -> (Callable<Future<String>>) () -> {
            Address address = state.faker.address();
            NameAndCity request = new NameAndCity(address.firstName(), address.lastName(), address.cityName());
            return state.executor.submit(() -> "Hello " + request.firstName + " from " + request.cityName);
        }).toList();
        return state.executor.invokeAll(tasks).stream().map(future -> {
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
