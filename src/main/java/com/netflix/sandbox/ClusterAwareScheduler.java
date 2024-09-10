package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * A virtual thread scheduler that uses a {@link ForkJoinPool} per cluster.
 * <p>
 * There are scheduling concerns that can't currently be handled by custom schedulers:
 * <ul>
 * <li> Managed blocking/compensation is handled by CarrierThread
 * <li> Yield uses <a href="https://github.com/openjdk/jdk/pull/11533/files">external submission to avoid the current worker queue</a>
 * </ul>
 */
public class ClusterAwareScheduler implements Executor {

    private final List<BitSet> clusters;
    private final ForkJoinPool[] pools;

    /**
     * Creates a {@link ClusterAwareScheduler} with parallelism and clusters
     * automatically using the currently online CPUs and the highest level cache
     * shared between those CPUs.
     */
    public ClusterAwareScheduler() {
        this(onlineClusters());
    }

    private static List<BitSet> onlineClusters() {
        return LinuxScheduling.onlineCpus()
            .mapToObj(LinuxScheduling::sharedCpus).map(cpus -> {
                BitSet bs = new BitSet();
                cpus.forEach(bs::set);
                return bs;
            }).distinct()
            .toList();
    }

    /**
     * Creates a {@link ClusterAwareScheduler} with parallelism and clusters
     * determined by the provided {@link BitSet}s.
     */
    public ClusterAwareScheduler(List<BitSet> clusters) {
        this.clusters = Objects.requireNonNull(clusters);
        if (clusters.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster must be provided");
        }
        pools = IntStream.range(0, clusters.size())
            .mapToObj(i -> createClusteredForkJoinPool(i, clusters.get(i)))
            .toArray(ForkJoinPool[]::new);
    }

    /**
     * Return the {@link ForkJoinPool}s.
     */
    public List<ForkJoinPool> getPools() {
        return List.of(pools);
    }

    /**
     * Return the clusters.
     */
    public List<BitSet> getClusters() {
        return List.copyOf(clusters);
    }

    private static final ThreadLocal<AtomicInteger> SUBMISSION_COUNT =
        ThreadLocal.withInitial(AtomicInteger::new);

    @Override
    public void execute(Runnable command) {
        ForkJoinPool pool;
        Thread ct = Thread.currentThread();
        if (ct instanceof ClusteredForkJoinWorkerThread t) {
            pool = pools[t.clusterIndex];
        } else {
            // so far for this exercise we're only concerned with the benefits of clustering
            // for throughput stress tests so we just round robin from the submission thread
            int i = SUBMISSION_COUNT.get().incrementAndGet() % pools.length;
            pool = pools[i];
        }
        pool.execute(command);
    }

    private static ForkJoinPool createClusteredForkJoinPool(int clusterIndex, BitSet cpus) {
        int parallelism = cpus.cardinality();
        if (parallelism == 0) {
            throw new IllegalArgumentException("At least one cpu must be defined");
        }
        int minRunnable = Integer.max(parallelism / 2, 1); // TODO copied from VirtualThread.createDefaultForkJoinPoolScheduler, related to compensation IIUC
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> new ClusteredForkJoinWorkerThread(pool, clusterIndex, cpus);
        Thread.UncaughtExceptionHandler handler = (_, _) -> {
        };
        boolean asyncMode = true;
        return new ForkJoinPool(parallelism, factory, handler, asyncMode,
            0, parallelism, minRunnable, _ -> true, 30, TimeUnit.SECONDS);
    }

    private static final class ClusteredForkJoinWorkerThread extends ForkJoinWorkerThread {
        private final int clusterIndex;
        private final BitSet cpus;

        private ClusteredForkJoinWorkerThread(ForkJoinPool pool, int clusterIndex, BitSet cpus) {
            super(null, pool, true);
            this.clusterIndex = clusterIndex;
            this.cpus = cpus;
        }

        @Override
        protected void onStart() {
            LinuxScheduling.currentThreadAffinity(cpus);
        }
    }

}
