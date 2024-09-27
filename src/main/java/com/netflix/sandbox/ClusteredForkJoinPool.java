package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class ClusteredForkJoinPool implements Executor {
    private final ExecutorService pool;
    
    public ClusteredForkJoinPool() {
        pool = ClusteredExecutors.newLeastLoadedPool(ClusteredExecutors::newWorkStealingPool);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
