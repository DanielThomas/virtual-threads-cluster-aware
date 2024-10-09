package com.netflix.sandbox;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class WorkStealingExecutor implements Executor {
    private final ExecutorService pool;

    public WorkStealingExecutor() {
        ClusteredExecutors.Strategy strategy = new ClusteredExecutors.Strategy(ClusteredExecutors.Placement.CHOOSE_TWO, Duration.ofMillis(500), Duration.ZERO);
        pool = ClusteredExecutors.newWorkStealingPool(strategy);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
