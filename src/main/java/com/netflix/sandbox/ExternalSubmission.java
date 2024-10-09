package com.netflix.sandbox;

import com.netflix.sandbox.ClusteredExecutors.Placement;
import com.netflix.sandbox.ClusteredExecutors.Strategy;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExternalSubmission {
    
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"BIASED", "CURRENT", "CHOOSE_TWO", "ROUND_ROBIN", "FJP"})
        public String placement;

        public ExecutorService executor;
        
        public Callable<String> task = () -> "Hello there.";

        @Setup(Level.Trial)
        public void setupExecutor() {
            if (placement.equals("FJP")) {
                executor = Executors.newWorkStealingPool();
                return;
            }
            executor = ClusteredExecutors.newWorkStealingPool(Strategy.withPlacement(Placement.valueOf(placement)));
        }
    }

    @Benchmark
    public String submit(BenchmarkState state) throws Exception {
        return state.executor.submit(state.task).get();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(ExternalSubmission.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

}
