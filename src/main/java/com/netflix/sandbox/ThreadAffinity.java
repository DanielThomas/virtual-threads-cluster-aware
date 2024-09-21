package com.netflix.sandbox;

import java.util.BitSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

public class ThreadAffinity {

    private ThreadAffinity() {
    }

    public static ForkJoinPool.ForkJoinWorkerThreadFactory forkJoinWorkerThreadFactory() {
        ThreadAffinityState state = new ThreadAffinityState();
        return pool -> new ForkJoinWorkerThread(pool) {
            protected void onStart() {
                super.onStart();
                state.onStart();
            }

            protected void onTermination(Throwable exception) {
                state.onTermination();
                super.onTermination(exception);
            }
        };
    }

    private static class ThreadAffinityState {
        private final BitSet[] clusters;
        private final int[] counts;
        private int current;

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

        private void onStart() {
            synchronized (this) {
                int min = Integer.MAX_VALUE;
                for (int i = 0; i < clusters.length; i++) {
                    int count = counts[i];
                    int numProcs = clusters[i].cardinality();
                    if (count < numProcs) {
                        current = i;
                        break;
                    }
                    if (count < min) {
                        // TODO consider numProcs too for BIGlittle architectures
                        current = i;
                        min = count;
                    }
                }
                counts[current]++;
                LinuxScheduling.currentThreadAffinity(clusters[current]);
            }
        }

        private void onTermination() {
            synchronized (this) {
                counts[current]--;
            }
        }
    }

}
