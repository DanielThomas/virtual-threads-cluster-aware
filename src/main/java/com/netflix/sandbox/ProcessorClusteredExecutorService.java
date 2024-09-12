package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An {@link ExecutorService} that attempts to place tasks on the current processor cluster.
 */
public class ProcessorClusteredExecutorService extends AbstractExecutorService {
    private final ForkJoinPool[] pools;
    private final int[][] adjacentClusterIndexes;
    private final int[] clusterIndexesByCpu;

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically determined the currently available processors and the highest level cache
     * shared between those processors.
     */
    public ProcessorClusteredExecutorService() {
        List<BitSet> clusters = LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::sharedProcessors).map(cpus -> {
                BitSet bs = new BitSet();
                cpus.forEach(bs::set);
                return bs;
            }).distinct()
            .toList();

        Objects.requireNonNull(clusters);
        int highestCpu = clusters.stream().mapToInt(BitSet::size).max().getAsInt();
        int numPools = clusters.size();
        pools = new ForkJoinPool[numPools];
        clusterIndexesByCpu = new int[highestCpu];

        Iterator<BitSet> it = clusters.iterator();
        IntStream.range(0, clusters.size()).forEach(i -> {
            BitSet processors = it.next();
            processors.stream().forEach(cpu -> clusterIndexesByCpu[cpu] = i);
            pools[i] = createForkJoinPool(i, processors);
        });

        int[][] clusterNodes = clusters.stream()
            .map(BitSet::stream)
            .map(i -> i.flatMap(LinuxScheduling::processorNodes).distinct().toArray())
            .toArray(int[][]::new);
        adjacentClusterIndexes = IntStream.range(0, clusterNodes.length).mapToObj(i -> {
            int[] nodes = clusterNodes[i];
            return IntStream.range(0, clusterNodes.length)
                .filter(j -> j != i && Arrays.equals(nodes, clusterNodes[j]))
                .toArray();
        }).toArray(int[][]::new);
    }

    /**
     * Return the {@link ForkJoinPool}s.
     */
    List<ForkJoinPool> getPools() {
        return List.of(pools);
    }

    @Override
    public void execute(Runnable command) {
        Thread ct = Thread.currentThread();
        int clusterIndex;
        if (ct instanceof ClusteredForkJoinWorkerThread t) {
            clusterIndex = t.clusterIndex;
        } else {
            clusterIndex = clusterIndexesByCpu[LinuxScheduling.currentProcessor()];
        }
        ForkJoinPool pool = pools[clusterIndex];
        long queueCount = pool.getQueuedTaskCount() + pool.getQueuedSubmissionCount();
        if (queueCount > 0) {
            // least loaded choice of two, with one being the current cluster
            int[] candidates = adjacentClusterIndexes[clusterIndex];
            int adjacentClusterIndex = candidates[ThreadLocalRandom.current().nextInt(candidates.length)];
            ForkJoinPool adjacentPool = pools[adjacentClusterIndex];
            long adjacentQueueCount = adjacentPool.getQueuedTaskCount() + adjacentPool.getQueuedSubmissionCount();
            if (queueCount > adjacentQueueCount) {
                pool = adjacentPool;
            }
        }
        pool.submit(command);
    }

    @Override
    public void shutdown() {
        Arrays.stream(pools).forEach(ForkJoinPool::shutdown);
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Arrays.stream(pools).flatMap(pool -> pool.shutdownNow().stream()).toList();
    }

    @Override
    public boolean isShutdown() {
        return Arrays.stream(pools).allMatch(ForkJoinPool::isShutdown);
    }

    @Override
    public boolean isTerminated() {
        return Arrays.stream(pools).allMatch(ForkJoinPool::isTerminated);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long current = System.nanoTime();
        long remaining = unit.toNanos(timeout);
        for (ForkJoinPool pool : pools) {
            if (!pool.awaitTermination(remaining, unit)) {
                return false;
            }
            long elapsed = current - System.nanoTime();
            remaining = remaining - elapsed;
        }
        return true;
    }

    @Override
    public String toString() {
        return IntStream.range(0, pools.length)
            .mapToObj(i -> i + ": " + pools[i])
            .collect(Collectors.joining("\n"));
    }

    private static ForkJoinPool createForkJoinPool(int clusterIndex, BitSet processors) {
        int parallelism = processors.cardinality();
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
        private final BitSet processors;

        private ClusteredForkJoinWorkerThread(ForkJoinPool pool, int clusterIndex, BitSet processors) {
            super(null, pool, true);
            this.clusterIndex = clusterIndex;
            this.processors = processors;
        }

        @Override
        protected void onStart() {
            LinuxScheduling.currentThreadAffinity(processors);
        }
    }

}
