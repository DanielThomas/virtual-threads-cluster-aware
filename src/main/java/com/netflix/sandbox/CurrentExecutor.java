package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.netflix.sandbox.ClusteredExecutors.PlacementStrategy.CURRENT;

public class CurrentExecutor implements Executor {
    private final ExecutorService pool;
    
    public CurrentExecutor() {
        pool = ClusteredExecutors.newClusteredPool(CURRENT, ClusteredExecutors::newWorkStealingPool);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
