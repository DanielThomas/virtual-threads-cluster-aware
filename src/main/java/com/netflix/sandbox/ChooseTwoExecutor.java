package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.netflix.sandbox.ClusteredExecutors.PlacementStrategy.CHOOSE_TWO;

public class ChooseTwoExecutor implements Executor {
    private final ExecutorService pool;
    
    public ChooseTwoExecutor() {
        pool = ClusteredExecutors.newClusteredPool(CHOOSE_TWO, ClusteredExecutors::newWorkStealingPool);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
