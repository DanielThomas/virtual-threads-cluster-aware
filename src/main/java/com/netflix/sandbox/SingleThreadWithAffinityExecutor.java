package com.netflix.sandbox;

import java.util.BitSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SingleThreadWithAffinityExecutor implements Executor {

    private final Executor executor;

    public SingleThreadWithAffinityExecutor() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(() -> {
                BitSet affinity = new BitSet();
                affinity.set(0);
                LinuxScheduling.currentThreadAffinity(affinity);
                r.run();
            });
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

}
