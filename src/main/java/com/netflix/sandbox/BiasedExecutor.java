package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.netflix.sandbox.ClusteredExecutors.PlacementStrategy.BIASED;

public class BiasedExecutor implements Executor {
    private final ExecutorService pool;
    
    public BiasedExecutor() {
        pool = ClusteredExecutors.newClusteredPool(BIASED, ClusteredExecutors::newWorkStealingPool);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}