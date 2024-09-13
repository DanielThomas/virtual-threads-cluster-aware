package com.netflix.sandbox;

import com.netflix.sandbox.ProcessorClusteredExecutorServiceBalancingBenchmark.BalancingBenchmarkState;
import com.netflix.sandbox.ProcessorClusteredExecutorServiceTaskBenchmark.TaskBenchmarkState;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

import static com.netflix.sandbox.VirtualThreadsSchedulerComparisonBenchmark.*;
import static org.junit.jupiter.api.Assertions.*;

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
                            executor.submit(() -> {
                            }).get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            ).get();

            assertEquals(1, executor.pools().stream().filter(pool -> pool.getStealCount() > 0).count());
            assertEquals(101, executor.pools().stream().mapToLong(ForkJoinPool::getStealCount).sum());
        }
    }

    @Test
    public void externalSubmissionCount() {
        try (ProcessorClusteredExecutorService executor = new ProcessorClusteredExecutorService()) {
            for (int i = 0; i < 1000; i++) {
                executor.submit(() -> {
                    try {
                        Thread.sleep(Duration.ofSeconds(1));
                    } catch (InterruptedException _) {
                    }
                });
            }

            int queuedSubmissionCount = executor.pools().stream().mapToInt(ForkJoinPool::getQueuedSubmissionCount).sum();
            assertEquals(1000 - Runtime.getRuntime().availableProcessors(), queuedSubmissionCount);
            executor.shutdownNow();
        }
    }

    @Test
    public void chooseLeastLoadedPoolBenchmark() {
        ProcessorClusteredExecutorServiceBalancingBenchmark benchmark = new ProcessorClusteredExecutorServiceBalancingBenchmark();
        BalancingBenchmarkState state = new BalancingBenchmarkState();
        state.setupExecutor();
        ForkJoinPool pool = benchmark.chooseLeastLoadedPool(state);

        assertNotNull(pool);
        assertTrue(pool.getQueuedSubmissionCount() > 10_000);
    }

    @Test
    public void externalBenchmark() throws InterruptedException {
        ProcessorClusteredExecutorServiceTaskBenchmark benchmark = new ProcessorClusteredExecutorServiceTaskBenchmark();
        TaskBenchmarkState state = new TaskBenchmarkState();
        state.setupExecutor();
        benchmark.external(state);
        System.out.println(state.executor.toString());
    }

    @Test
    public void internalBenchmark() throws ExecutionException, InterruptedException {
        ProcessorClusteredExecutorServiceTaskBenchmark benchmark = new ProcessorClusteredExecutorServiceTaskBenchmark();
        TaskBenchmarkState state = new TaskBenchmarkState();
        state.setupExecutor();
        benchmark.internal(state);
        System.out.println(state.executor.toString());
    }

    @Test
    public void submitBenchmark() throws InterruptedException, ExecutionException {
        VirtualThreadsSchedulerComparisonBenchmark benchmark = new VirtualThreadsSchedulerComparisonBenchmark();
        SchedulerBenchmarkState state = new SchedulerBenchmarkState();
        state.scheduler = Scheduler.CLUSTERED;
        state.setupExecutor();
        String actual = benchmark.submit(state);

        assertTrue(actual.startsWith("Hello"));
    }

    @Test
    public void invokeAllBenchmark() throws InterruptedException {
        VirtualThreadsSchedulerComparisonBenchmark benchmark = new VirtualThreadsSchedulerComparisonBenchmark();
        SchedulerBenchmarkState state = new SchedulerBenchmarkState();
        state.scheduler = Scheduler.CLUSTERED;
        state.setupExecutor();
        List<String> results = benchmark.invokeAll(state);

        assertEquals(state.numTasks, results.size());
    }

}
