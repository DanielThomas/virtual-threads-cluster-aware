package com.netflix.sandbox;

import com.netflix.sandbox.ClusteredExecutors.LoadBalancingStrategy;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.netflix.sandbox.ClusteredExecutors.LoadBalancingStrategy.BIASED;

public class BiasedExecutor implements Executor {
    private final ExecutorService pool;
    
    public BiasedExecutor() {
        pool = ClusteredExecutors.newLoadBalancingPool(BIASED, ClusteredExecutors::newWorkStealingPool);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }
}