package com.netflix.sandbox;

import org.junit.jupiter.api.Test;

import java.util.TreeSet;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusteredExecutorTest {

    @Test
    public void canExecute() throws InterruptedException {
        try (ExecutorService executor = new ClusteredExecutor()) {
            CountDownLatch latch = new CountDownLatch(1);
            executor.execute(latch::countDown);
            assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void doesClusterWorkerSubmissions() throws InterruptedException, ExecutionException {
        try (ClusteredExecutor executor = new ClusteredExecutor()) {
            try {
                executor.submit(() -> {
                        for (int i = 0; i < 100; i++) {
                            try {
                                executor.submit(() -> {
                                }).get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                ).get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }

//            assertEquals(1, executor.pools().stream().filter(pool -> pool.getStealCount() > 0).count());
//            assertEquals(101, executor.pools().stream().mapToLong(ForkJoinPool::getStealCount).sum());
        }
    }

    @Test
    public void submitBenchmark() throws InterruptedException, ExecutionException {
        ClusteredExecutorTaskBenchmark benchmark = new ClusteredExecutorTaskBenchmark();
        ClusteredExecutorTaskBenchmark.TaskBenchmarkState state = new ClusteredExecutorTaskBenchmark.TaskBenchmarkState();
        state.setupExecutor();
        int result = benchmark.submit(state);

        assertEquals(0, result);
    }

}
