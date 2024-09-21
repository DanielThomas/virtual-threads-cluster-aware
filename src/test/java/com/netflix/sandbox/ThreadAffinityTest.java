package com.netflix.sandbox;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;

public class ThreadAffinityTest {

    @Test
    public void forkJoinFactory() throws ExecutionException, InterruptedException {
        ForkJoinWorkerThreadFactory factory = ThreadAffinity.forkJoinWorkerThreadFactory();
        try (ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), factory, null, false)) {
            for(int i = 0; i < 1000; i++) {
                pool.submit(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                });   
            }
        }
        Thread.sleep(600000);
    }

}