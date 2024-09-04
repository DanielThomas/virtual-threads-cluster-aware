package com.netflix.sandbox;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ClusterAwareBenchmark {

    @State(Scope.Benchmark)
    public static class ExecutorState {
        @Param({"PLATFORM", "VIRTUAL"})
        public ThreadType threadType;

        @Param({"true", "false"})
        public boolean taskset;
        
        public ExecutorService executor;

        public int numTasks;
        
        public Faker faker = new Faker();

        @Setup(Level.Trial)
        public void setupExecutor() throws IOException, InterruptedException {
            String cpuSet = ExecutorState.cpuSet();
            numTasks = Integer.parseInt(cpuSet.split("-")[1]);
            System.setProperty("jdk.virtualThreadScheduler.parallelism", String.valueOf(numTasks));
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(numTasks));
            executor = switch (threadType) {
                case PLATFORM -> ForkJoinPool.commonPool();
                case VIRTUAL -> Executors.newVirtualThreadPerTaskExecutor();
            };
            if (taskset) {
                long pid = ProcessHandle.current().pid();
                Process process = new ProcessBuilder("taskset", "-a", "-cp", cpuSet, String.valueOf(pid)).start();
                process.waitFor();
            }
        }

        public static String cpuSet() {
            Path path = Path.of("/sys/devices/system/cpu/cpu0/cache");
            try (Stream<Path> dirs = Files.list(path).filter(Files::isDirectory)) {
                Optional<String> firstSharedCache = dirs.filter(dir -> dir.getFileName().toString().startsWith("index"))
                    .sorted()
                    .map(dir -> {
                        try {
                            return Files.readString(dir.resolve("shared_cpu_list"));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).filter(sharedCpuList -> sharedCpuList.contains("-"))
                    .findFirst();
                if (firstSharedCache.isEmpty()) {
                    throw new IllegalStateException("No caches are shared between other cpus");
                }
                return firstSharedCache.get().trim();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
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
            .include(ClusterAwareBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

    public enum ThreadType {
        PLATFORM,
        VIRTUAL
    }

}
