package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An {@link ExecutorService} that attempts to place tasks on the current processor cluster.
 */
public class ProcessorClusteredExecutorService extends AbstractExecutorService {
    static final Function<ForkJoinPool, Long> DEFAULT_LOAD_FUNCTION = (pool) -> pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount();

    private final ForkJoinPool[] pools;
    private final int[][] neighbourClusterIndexes;
    private final int[][] adjacentClusterIndexes;
    private final int[] clusterIndexesByProcessor;
    private final boolean clusterSubmissions;

    private final AtomicInteger foo = new AtomicInteger();

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically determined the currently available processors and the highest level cache
     * shared between those processors.
     */
    public ProcessorClusteredExecutorService() {
        this(true);
    }

    /**
     * Creates a {@link ProcessorClusteredExecutorService} with parallelism and clusters
     * automatically determined the currently available processors and the highest level cache
     * shared between those processors.
     *
     * @param clusterSubmissions should submissions from threads external to this executor
     * service have affinity to the current cluster
     */
    public ProcessorClusteredExecutorService(boolean clusterSubmissions) {
        this.clusterSubmissions = clusterSubmissions;

        List<BitSet> clusters = LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::sharedProcessors).map(cpus -> {
                BitSet bs = new BitSet();
                cpus.forEach(bs::set);
                return bs;
            }).distinct()
            .toList();

        Objects.requireNonNull(clusters);
        int highestProcessor = clusters.stream().mapToInt(BitSet::size).max().getAsInt();
        int numPools = clusters.size();
        pools = new ForkJoinPool[numPools];
        clusterIndexesByProcessor = new int[highestProcessor];
        Arrays.fill(clusterIndexesByProcessor, -1);

        Iterator<BitSet> it = clusters.iterator();
        IntStream.range(0, clusters.size()).forEach(i -> {
            BitSet processors = it.next();
            processors.stream().forEach(n -> clusterIndexesByProcessor[n] = i);
            pools[i] = createForkJoinPool(i, processors);
        });

        adjacentClusterIndexes = IntStream.range(0, clusters.size())
            .mapToObj(i -> IntStream.range(0, clusters.size())
                .filter(j -> j != i)
                .toArray())
            .toArray(int[][]::new);

        int[][] clusterNodes = clusters.stream()
            .map(BitSet::stream)
            .map(i -> i.flatMap(LinuxScheduling::processorNodes).distinct().toArray())
            .toArray(int[][]::new);
        neighbourClusterIndexes = IntStream.range(0, clusterNodes.length).mapToObj(i -> {
            int[] nodes = clusterNodes[i];
            return IntStream.range(0, clusterNodes.length)
                .filter(j -> j != i && Arrays.equals(nodes, clusterNodes[j]))
                .toArray();
        }).toArray(int[][]::new);
    }

    /**
     * Return the {@link ForkJoinPool}s.
     */
    List<ForkJoinPool> pools() {
        return List.of(pools);
    }

    @Override
    public void execute(Runnable command) {
        foo.incrementAndGet();
        Thread t = Thread.currentThread();
        ForkJoinPool preferredPool; int[] candidateIndexes;
        if (t instanceof ClusteredForkJoinWorkerThread ct) {
            int clusterIndex = ct.clusterIndex;
            preferredPool = pools[clusterIndex];
            if (ForkJoinTask.getSurplusQueuedTaskCount() <= preferredPool.getParallelism()) {
                // Alan mentioned ForkJoinWorkerThread.hasKnownQueuedWork might work here: https://mail.openjdk.org/pipermail/loom-dev/2024-September/007192.html
                // but this allows us to be sticker to the current cluster, and give the worker the best possible chance of keeping tasks local
                preferredPool.submit(command);
                return;
            }
            candidateIndexes = neighbourClusterIndexes[clusterIndex];
        } else {
            int clusterIndex;
            if (clusterSubmissions) {
                clusterIndex = clusterIndexesByProcessor[LinuxScheduling.currentProcessor()];
                candidateIndexes = neighbourClusterIndexes[clusterIndex];
            } else {
                clusterIndex = ThreadLocalRandom.current().nextInt(pools.length);
                candidateIndexes = adjacentClusterIndexes[clusterIndex];
            }
            preferredPool = pools[clusterIndex];
        }
        ForkJoinPool pool = chooseLeastLoadedPool(preferredPool, candidateIndexes, DEFAULT_LOAD_FUNCTION);
        pool.submit(command);
    }

    /**
     * Choose the least loaded pool from an array of {@link ForkJoinPool},
     * with the first choice a provided preferred pool.
     *
     * @param preferredPool the preferred {@link ForkJoinPool}
     * @param candidateIndexes the candidate array indexes when selecting the alternative to the {@code preferredPool}.
     *                         omitting the index of the {@code preferredPool}
     * @param poolLoad a function to compute the pool load
     */
    ForkJoinPool chooseLeastLoadedPool(ForkJoinPool preferredPool, int[] candidateIndexes, Function<ForkJoinPool, Long> poolLoad) {
        ForkJoinPool pool = preferredPool;
        long load = poolLoad.apply(preferredPool);
        if (load > 0) {
            int otherIndex = candidateIndexes[ThreadLocalRandom.current().nextInt(candidateIndexes.length)];
            ForkJoinPool otherPool = pools[otherIndex];
            long otherLoad = poolLoad.apply(otherPool);
            if (load > otherLoad) {
                pool = otherPool;
            }
        }
        return pool;
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
