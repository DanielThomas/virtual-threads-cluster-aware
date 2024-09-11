package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An {@link ExecutorService} that attempts to place tasks to the nearest processor cluster.
 */
public class ProcessorClusteredExecutorService extends AbstractExecutorService {
    private final ForkJoinPool[] pools;
    private final int[][] candidatePools;
    private final int[] clustersByCpu;

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically determined the currently available processors and the highest level cache
     * shared between those processors.
     */
    public ProcessorClusteredExecutorService() {
        this(sharedProcessors());
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
    protected ProcessorClusteredExecutorService(SequencedCollection<BitSet> clusters) {
        Objects.requireNonNull(clusters);
        int highestCpu = clusters.stream().mapToInt(BitSet::size).max().getAsInt();
        // TODO check that clusters don't overlap
        // TODO check that clusters have equal sizes
        int numPools = clusters.size();
        pools = new ForkJoinPool[numPools];
        candidatePools = new int[numPools][];
        clustersByCpu = new int[highestCpu];
        Iterator<BitSet> it = clusters.iterator();
        IntStream.range(0, clusters.size()).forEach(i -> {
            BitSet processors = it.next();
            processors.stream().forEach(cpu -> clustersByCpu[cpu] = i);
            pools[i] = createForkJoinPool(i, processors);
            candidatePools[i] = IntStream.range(0, numPools).filter(j -> j == i).toArray();
        });
    }

    /**
     * Return the {@link ForkJoinPool}s.
     */
    List<ForkJoinPool> getPools() {
        return List.of(pools);
    }

    @Override
    public void execute(Runnable command) {
        ForkJoinPool pool;
        Thread ct = Thread.currentThread();
        int clusterIndex;
        if (ct instanceof ClusteredForkJoinWorkerThread t) {
            clusterIndex = t.clusterIndex;
            pool = pools[clusterIndex];
            if (t.getQueuedTaskCount() == 0) {
                pool.submit(command);
                return;
            }
        } else {
            clusterIndex = clustersByCpu[LinuxScheduling.currentProcessor()];
            pool = pools[clusterIndex];
        }
        int queuedSumissions = pool.getQueuedSubmissionCount();
        if (queuedSumissions > 0) {
            // least loaded choice of two, with one being the current cluster
            int[] candidates = candidatePools[clusterIndex];
            int otherClusterIndex = candidates[ThreadLocalRandom.current().nextInt(candidates.length)];
            ForkJoinPool otherPool = pools[otherClusterIndex];
            if (queuedSumissions > otherPool.getQueuedSubmissionCount()) {
                pool = otherPool;
            }
        }
        pool.externalSubmit(ForkJoinTask.adapt(command));
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
