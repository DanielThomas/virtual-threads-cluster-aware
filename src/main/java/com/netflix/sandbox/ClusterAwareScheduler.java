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
    private final ForkJoinPool[] poolsByCluster;
    private final ForkJoinPool[] poolsByCpu;
    private final int queueBalancingThreshold;

    /**
     * Creates a {@link ClusterAwareScheduler} with parallelism and clusters
     * automatically using the currently online CPUs and the highest level cache
     * shared between those CPUs.
     */
    public ClusterAwareScheduler() {
        this(sharedCpuClusters());
    }

    private static List<BitSet> sharedCpuClusters() {
        return LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::sharedProcessors).map(cpus -> {
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
    protected ClusterAwareScheduler(List<BitSet> clusters) {
        this.clusters = Objects.requireNonNull(clusters);
        if (clusters.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster must be provided");
        }
        // TODO check clusters do not overlap
        // TODO check cluster does not have zero cpus
        queueBalancingThreshold = clusters.stream().mapToInt(BitSet::cardinality)
            .distinct()
            .reduce((a, b) -> {
                throw new IllegalStateException("Clusters have uneven numbers of cpus: " + a + ", " + b);
            }).getAsInt();
        int numCpus = clusters.stream().mapToInt(BitSet::length).max().getAsInt();
        poolsByCpu = new ForkJoinPool[numCpus];
        poolsByCluster = new ForkJoinPool[clusters.size()];
        IntStream.range(0, clusters.size()).forEach(i -> {
            BitSet cluster = clusters.get(i);
            poolsByCluster[i] = createClusteredForkJoinPool(i, cluster);
            cluster.stream().forEach(ci -> poolsByCpu[ci] = poolsByCluster[i]);
        });
    }

    /**
     * Return the {@link ForkJoinPool}s.
     */
    public List<ForkJoinPool> getPools() {
        return List.of(poolsByCluster);
    }

    /**
     * Return the clusters.
     */
    public List<BitSet> getClusters() {
        return List.copyOf(clusters);
    }

    @Override
    public void execute(Runnable command) {
        ForkJoinPool pool;
        Thread ct = Thread.currentThread();
        if (ct instanceof ClusteredForkJoinWorkerThread t) {
            pool = poolsByCluster[t.clusterIndex];
        } else {
            int cpu = LinuxScheduling.currentProcessor();
            pool = poolsByCpu[cpu];
        }
        int queuedSubmissionCount = pool.getQueuedSubmissionCount();
        if (queuedSubmissionCount > queueBalancingThreshold) {
            // least loaded
            for (ForkJoinPool candidate : poolsByCluster) {
                if (pool == candidate) continue;
                int candidateQueuedSubmissionCount = candidate.getQueuedSubmissionCount();
                if (candidateQueuedSubmissionCount == 0) {
                    pool = candidate;
                    break;
                }
                if (candidateQueuedSubmissionCount < queuedSubmissionCount) {
                    pool = candidate;
                    queuedSubmissionCount = candidateQueuedSubmissionCount;
                }
            }
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
