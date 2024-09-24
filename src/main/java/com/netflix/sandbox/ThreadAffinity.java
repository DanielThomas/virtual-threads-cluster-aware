package com.netflix.sandbox;

import java.util.BitSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;

public class ThreadAffinity {

    private ThreadAffinity() {
    }

    public static ThreadFactory defaultThreadFactory() {
        return wrap(Executors.defaultThreadFactory());
    }

    public static ThreadFactory wrap(ThreadFactory factory) {
        ThreadAffinityState state = new ThreadAffinityState();
        return r -> factory.newThread(() -> {
            try {
                state.threadStarted();
                r.run();
            } finally {
                state.threadTerminated();
            }
        });
    }

    public static ForkJoinPool.ForkJoinWorkerThreadFactory forkJoinWorkerThreadFactory() {
        ThreadAffinityState state = new ThreadAffinityState();
        return pool -> new ForkJoinWorkerThread(pool) {
            protected void onStart() {
                super.onStart();
                state.threadStarted();
            }

            protected void onTermination(Throwable exception) {
                state.threadTerminated();
                super.onTermination(exception);
            }
        };
    }

    private static class ThreadAffinityState {
        private final BitSet[] clusters;
        private final int[] counts;
        private int affinity;

        private ThreadAffinityState() {
            clusters = LinuxScheduling.availableProcessors()
                .mapToObj(LinuxScheduling::llcSharedProcessors)
                .map(i -> {
                    BitSet cpus = new BitSet();
                    i.forEach(cpus::set);
                    return cpus;
                }).distinct()
                .toArray(BitSet[]::new);
            counts = new int[clusters.length];
        }

        private void threadStarted() {
            synchronized (this) {
                int min = Integer.MAX_VALUE;
                for (int i = 0; i < clusters.length; i++) {
                    int count = counts[i];
                    int numProcs = clusters[i].cardinality();
                    if (count < numProcs) {
                        affinity = i;
                        break;
                    }
                    if (count < min) {
                        // TODO consider numProcs too for BIGlittle architectures
                        affinity = i;
                        min = count;
                    }
                }
                counts[affinity]++;
                LinuxScheduling.currentThreadAffinity(clusters[affinity]);
                Thread t = Thread.currentThread();
                t.setName(t.getName() + "-affinity-" + affinity);
            }
        }

        private void threadTerminated() {
            synchronized (this) {
                counts[affinity]--;
            }
        }
    }

}
