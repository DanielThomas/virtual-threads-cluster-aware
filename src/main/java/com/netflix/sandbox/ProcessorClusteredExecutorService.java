package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An {@link ExecutorService} that attempts to place tasks to the nearest processor cluster.
 */
public class ProcessorClusteredExecutorService extends AbstractExecutorService {
    private final List<BitSet> clusters;
    private final ForkJoinPool[] poolsByCluster;
    private final ForkJoinPool[] poolsByCpu;
    private final BalancingStrategy balancingStrategy;

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically using the currently available processors and the highest level cache
     * shared between those CPUs.
     * <p>
     * The {@link BalancingStrategy#LEAST_LOADED} strategy is used unless it is
     * configured using the {@code netflix.ClusterAwareScheduler.balancingStrategy}
     * system property.
     */
    public ProcessorClusteredExecutorService() {
        this(defaultBalancingStrategy());
    }

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically using the currently available processors and the highest level cache
     * shared between those processors.
     */
    public ProcessorClusteredExecutorService(BalancingStrategy balancingStrategy) {
        this(sharedProcessors(), balancingStrategy);
    }

    private static BalancingStrategy defaultBalancingStrategy() {
        String name = System.getProperty("netflix.ClusterAwareScheduler.balancingStrategy");
        if (name != null) {
            BalancingStrategy.valueOf(name);
        }
        return BalancingStrategy.LEAST_LOADED;
    }

    protected static List<BitSet> sharedProcessors() {
        return LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::sharedProcessors).map(cpus -> {
                BitSet bs = new BitSet();
                cpus.forEach(bs::set);
                return bs;
            }).distinct()
            .toList();
    }

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * determined by the provided {@link BitSet}s.
     */
    protected ProcessorClusteredExecutorService(List<BitSet> clusters, BalancingStrategy balancingStrategy) {
        this.clusters = Objects.requireNonNull(clusters);
        this.balancingStrategy = Objects.requireNonNull(balancingStrategy);
        if (clusters.isEmpty()) {
            throw new IllegalArgumentException("At least one cluster must be provided");
        }
        // TODO check clusters do not overlap
        // TODO check cluster does not have zero cpus
        // TODO check clusters are balanced
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
    List<ForkJoinPool> getPools() {
        return List.of(poolsByCluster);
    }

    /**
     * Return the clusters.
     */
    List<BitSet> getClusters() {
        return List.copyOf(clusters);
    }

    @Override
    public void execute(Runnable command) {
        ForkJoinPool pool;
        Thread ct = Thread.currentThread();
        if (ct instanceof ClusteredForkJoinWorkerThread t) {
            pool = poolsByCluster[t.clusterIndex];
            if (t.getQueuedTaskCount() == 0) {
                pool.submit(command);
                return;
            }
        } else {
            int cpu = LinuxScheduling.currentProcessor();
            pool = poolsByCpu[cpu];
        }
        if (balancingStrategy == BalancingStrategy.LEAST_LOADED) {
            int queuedSubmissionCount = pool.getQueuedSubmissionCount();
            if (queuedSubmissionCount > 0) {
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
        }
        pool.externalSubmit(ForkJoinTask.adapt(command));
    }

    @Override
    public void shutdown() {

    }

    @Override
    public List<Runnable> shutdownNow() {
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO
        return true;
    }

    @Override
    public String toString() {
        return IntStream.range(0, poolsByCluster.length)
            .mapToObj(i -> i + ": " + poolsByCluster[i])
            .collect(Collectors.joining("\n"));
    }

    private static ForkJoinPool createClusteredForkJoinPool(int clusterIndex, BitSet processors) {
        int parallelism = processors.cardinality();
        if (parallelism == 0) {
            throw new IllegalArgumentException("At least one cpu must be defined");
        }
        int minRunnable = Integer.max(parallelism / 2, 1); // TODO copied from VirtualThread.createDefaultForkJoinPoolScheduler, related to compensation IIUC
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> new ClusteredForkJoinWorkerThread(pool, clusterIndex, processors);
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

    public enum BalancingStrategy {
        NONE,
        LEAST_LOADED,
        BIASED_CHOOSE_TWO
    }

}
