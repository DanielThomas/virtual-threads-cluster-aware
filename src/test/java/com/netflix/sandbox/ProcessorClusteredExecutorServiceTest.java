package com.netflix.sandbox;

import com.netflix.sandbox.ProcessorClusteredExecutorServiceBalancingBenchmark.BalancingBenchmarkState;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessorClusteredExecutorServiceTest {

    @Test
    public void canExecute() throws InterruptedException {
        try (ExecutorService executor = new ProcessorClusteredExecutorService()) {
            CountDownLatch latch = new CountDownLatch(1);
            executor.execute(latch::countDown);
            assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void doesClusterWorkerSubmissions() throws InterruptedException, ExecutionException {
        try (ProcessorClusteredExecutorService executor = new ProcessorClusteredExecutorService()) {
            executor.submit(() -> {
                    for (int i = 0; i < 100; i++) {
                        try {
                            executor.submit(() -> {}).get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            ).get();

            assertEquals(1, executor.getPools().stream().filter(pool -> pool.getStealCount() > 0).count());
            assertEquals(101, executor.getPools().stream().mapToLong(ForkJoinPool::getStealCount).sum());
        }
    }

    @Test
    public void externalBenchmark() throws InterruptedException {
        ProcessorClusteredExecutorServiceBalancingBenchmark benchmark = new ProcessorClusteredExecutorServiceBalancingBenchmark();
        BalancingBenchmarkState state = new BalancingBenchmarkState();
        state.setupExecutor();
        benchmark.external(state);
        System.out.println(state.executor.toString());
    }

    @Test
    public void internalBenchmark() throws ExecutionException, InterruptedException {
        ProcessorClusteredExecutorServiceBalancingBenchmark benchmark = new ProcessorClusteredExecutorServiceBalancingBenchmark();
        BalancingBenchmarkState state = new BalancingBenchmarkState();
        state.setupExecutor();
        benchmark.internal(state);
        System.out.println(state.executor.toString());
    }

}
