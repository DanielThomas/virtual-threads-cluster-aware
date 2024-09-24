package com.netflix.sandbox;

import java.util.BitSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadAffinityForkJoinPool implements Executor {

    private final ForkJoinPool pool;

    public ThreadAffinityForkJoinPool() {
        this.pool = createForkJoinPool(Runtime.getRuntime().availableProcessors());
    }
    
    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }

    private static ForkJoinPool createForkJoinPool(int parallelism) {
        int minRunnable = Integer.max(parallelism / 2, 1); // TODO copied from VirtualThread.createDefaultForkJoinPoolScheduler, related to compensation IIUC
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = ThreadAffinity.forkJoinWorkerThreadFactory();
        Thread.UncaughtExceptionHandler handler = (_, _) -> {
        };
        boolean asyncMode = true;
        return new ForkJoinPool(parallelism, factory, handler, asyncMode,
            0, parallelism, minRunnable, _ -> true, 30, TimeUnit.SECONDS);
    }
    
}
