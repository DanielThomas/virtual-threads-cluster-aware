package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class DefaultForkJoinPool implements Executor {
    
    private ForkJoinPool pool;
    
    public DefaultForkJoinPool() {
        String value = System.getProperty("jdk.virtualThreadScheduler.parallelism");
        int parallelism;
        if (value != null) {
            parallelism = Integer.parseInt(value);
        } else {
            parallelism = Runtime.getRuntime().availableProcessors();
        }
        this.pool = createForkJoinPool(parallelism);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
    
    private static ForkJoinPool createForkJoinPool(int parallelism) {
        int minRunnable = Integer.max(parallelism / 2, 1); // TODO copied from VirtualThread.createDefaultForkJoinPoolScheduler, related to compensation IIUC
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
        Thread.UncaughtExceptionHandler handler = (_, _) -> {
        };
        boolean asyncMode = true;
        return new ForkJoinPool(parallelism, factory, handler, asyncMode,
            0, parallelism, minRunnable, _ -> true, 30, TimeUnit.SECONDS);
    }

}