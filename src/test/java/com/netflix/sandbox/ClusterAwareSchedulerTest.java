package com.netflix.sandbox;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class ClusterAwareSchedulerTest {

    @Test
    public void canExecute() throws InterruptedException {
        ClusterAwareScheduler scheduler = new ClusterAwareScheduler();
        CountDownLatch latch = new CountDownLatch(1);
        scheduler.execute(latch::countDown);
        Assertions.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void doesClusterWorkerSubmissions() throws InterruptedException {
        ClusterAwareScheduler scheduler = new ClusterAwareScheduler();
        CountDownLatch latch = new CountDownLatch(100);
        CopyOnWriteArrayList<Thread> mismatchedCluster = new CopyOnWriteArrayList<>();

        IntStream.range(0, (int) latch.getCount())
            .forEach(_ -> scheduler.execute(() -> {
                    ForkJoinPool parentPool = ((ForkJoinWorkerThread) Thread.currentThread()).getPool();
                    scheduler.execute(() -> {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ignored) {
                        }
                        ForkJoinWorkerThread currentWorker = (ForkJoinWorkerThread) Thread.currentThread();
                        if (currentWorker.getPool() != parentPool) {
                            mismatchedCluster.add(currentWorker);
                        }
                        latch.countDown();
                    });
                }
            ));
        Assertions.assertTrue(latch.await(1, TimeUnit.SECONDS));
        Assertions.assertTrue(mismatchedCluster.isEmpty());
    }
    
    @Test
    public void benchmarkRuns() throws InterruptedException {
        ClusterAwareSchedulerBenchmark benchmark = new ClusterAwareSchedulerBenchmark();
        ClusterAwareSchedulerBenchmark.ExecutorState state = new ClusterAwareSchedulerBenchmark.ExecutorState();
        state.scheduler = ClusterAwareSchedulerBenchmark.Scheduler.CLUSTERED;
        state.setupExecutor();
        benchmark.benchmark(state);
    }

}
