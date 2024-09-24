package com.netflix.sandbox;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ThreadAffinityTest {

    @Test
    public void defaultThreadFactory() {
        ThreadFactory factory = ThreadAffinity.defaultThreadFactory();
        int[] cpus = IntStream.range(0, Runtime.getRuntime().availableProcessors())
            .map(i -> {
                AtomicInteger cpu = new AtomicInteger();
                Thread t = factory.newThread(() -> cpu.set(LinuxScheduling.currentProcessor()));
                t.start();
                try {
                    t.join();
                } catch (InterruptedException _) {
                }
                return cpu.get();
            }).toArray();
        System.out.println(cpus);
    }

    @Test
    public void forkJoinPool() {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = ThreadAffinity.forkJoinWorkerThreadFactory();
        int parallelism = Runtime.getRuntime().availableProcessors();
        try (var pool = new ForkJoinPool(parallelism, factory, null, false)) {
            List<ForkJoinTask<Integer>> tasks = IntStream.range(0, parallelism).mapToObj(_ -> pool.submit(() -> {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return LinuxScheduling.currentProcessor();
            })).toList();
            List<Integer> results = tasks.stream().map(task -> {
                try {
                    return task.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }).toList();
            results.toString();
        }
    }

}
