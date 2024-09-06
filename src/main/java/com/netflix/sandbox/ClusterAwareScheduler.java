package com.netflix.sandbox;

import net.openhft.affinity.Affinity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
            pool = pools[SUBMISSION_COUNT.get().incrementAndGet() % (pools.length - 1)];
        }
        pool.execute(command);
    }

    static List<BitSet> onlineClusters() {
        if (!System.getProperty("os.name").equals("Linux")) {
            throw new IllegalStateException("Linux is required to infer cluster configuration");
        }
        try {
            return onlineCpus().mapToObj(cpuIndex -> {
                    try {
                        BitSet affinity = new BitSet();
                        sharedCpusForHighestLevelCache(cpuIndex).forEach(affinity::set);
                        return affinity;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }).distinct()
                .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static IntStream onlineCpus() throws IOException {
        Path path = Path.of("/sys/devices/system/cpu/online");
        String onlineCpus = Files.readString(path).trim();
        return parseCpuList(onlineCpus);
    }

    static IntStream sharedCpusForHighestLevelCache(int cpuIndex) throws IOException {
        Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuIndex + "/cache");
        try (Stream<Path> dirs = Files.list(path).filter(Files::isDirectory)) {
            int highestIndex = dirs.filter(dir -> dir.getFileName().toString().startsWith("index"))
                .mapToInt(dir -> {
                    String filename = dir.getFileName().toString();
                    return Integer.parseInt(filename.substring(5));
                }).max()
                .getAsInt();
            String sharedCpus = Files.readString(path.resolve("index" + highestIndex, "shared_cpu_list")).trim();
            return parseCpuList(sharedCpus);
        }
    }

    private static IntStream parseCpuList(String cpuList) {
        return Arrays.stream(cpuList.split(","))
            .flatMapToInt(s -> {
                String[] split = s.split("-");
                int start = Integer.parseInt(split[0]);
                if (split.length == 2) {
                    return IntStream.rangeClosed(start, Integer.parseInt(split[1]));
                }
                return IntStream.of(start);
            });
    }

    private static ForkJoinPool createClusteredForkJoinPool(int clusterIndex, BitSet affinity) {
        int parallelism = affinity.cardinality();
        if (parallelism == 0) {
            throw new IllegalArgumentException("At least one cpu must be defined");
        }
        int minRunnable = Integer.max(parallelism / 2, 1); // TODO copied from VirtualThread.createDefaultForkJoinPoolScheduler, related to compensation IIUC
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> new ClusteredForkJoinWorkerThread(pool, clusterIndex, affinity);
        Thread.UncaughtExceptionHandler handler = (_, _) -> {
        };
        boolean asyncMode = true;
        return new ForkJoinPool(parallelism, factory, handler, asyncMode,
            0, parallelism, minRunnable, _ -> true, 30, TimeUnit.SECONDS);
    }

    private static final class ClusteredForkJoinWorkerThread extends ForkJoinWorkerThread {
        private final int clusterIndex;
        private final BitSet affinity;

        private ClusteredForkJoinWorkerThread(ForkJoinPool pool, int clusterIndex, BitSet affinity) {
            super(null, pool, true);
            this.clusterIndex = clusterIndex;
            this.affinity = affinity;
        }

        @Override
        protected void onStart() {
            Affinity.setAffinity(affinity);
        }
    }

}
