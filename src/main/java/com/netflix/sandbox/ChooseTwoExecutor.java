package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.netflix.sandbox.ClusteredExecutors.PlacementStrategy.CHOOSE_TWO;

public class ChooseTwoExecutor implements Executor {
    private final ExecutorService pool;
    
    public ChooseTwoExecutor() {
        pool = ClusteredExecutors.newWorkStealingPool();
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
