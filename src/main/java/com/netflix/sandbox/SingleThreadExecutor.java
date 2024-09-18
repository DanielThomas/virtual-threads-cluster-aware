package com.netflix.sandbox;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SingleThreadExecutor implements Executor {

    private final Executor executor;

    public SingleThreadExecutor() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

}

