package com.netflix.sandbox;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class WorkStealingExecutor implements Executor {
    private final ExecutorService pool;

    public WorkStealingExecutor() {
        pool = ClusteredExecutors.newWorkStealingPool();
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}
