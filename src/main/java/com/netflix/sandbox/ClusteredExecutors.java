package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class counterpart to {@link java.util.concurrent.Executors}, ...
 * <p>
 * These utilities are intended for use on machines with exclusive use of the available processors,
 * and methods check that available processors do not change over time.
 * <p>
 * ...
 * <ul>
 * <li> Mutual-exclusivity - threads on a given cluster will not run* on the same processors as those in another
 * <li> Locality - threads in the same cluster will be scheduled such that they always share a last-level cache
 * </ul>
 * <p>
 * Note: The way threads in created {@link ExecutorService} instances will differ depending on the capabilities of the
 * underlying scheduler. On platforms where thread CPU affinity is used, the result of {@link Runtime#availableProcessors()}
 * may be affected on threads with affinity set.
 *
 * @author Danny Thomas
 * @since 1.0
 */
public class ClusteredExecutors {

    /**
     * Returns the number of processors clusters available to the Java virtual machine.
     *
     * @return the number of processor clusters, at least one
     * @throws IllegalStateException if the the number of processors available to the current thread are different
     *                               than {@link Runtime#availableProcessors()} observed when this class was loaded.
     */
    public static int availableClusters() {
        checkInvariants();
        return (int) clusters().count();
    }

    /**
     * Returns the number of processors available to the given cluster index.
     */
    public static int availableProcessors(int cluster) {
        checkInvariants();
        checkClusterIndex(cluster);
        return clusters().toList()
            .get(cluster)
            .cardinality();
    }

    /**
     * Creates a work-stealing thread pool...
     */
    public static ExecutorService newWorkStealingPool(int cluster) {
        checkInvariants();
        checkClusterIndex(cluster);
        ForkJoinPool pool = new ForkJoinPool
            (availableProcessors(cluster),
                forkJoinWorkerThreadFactory(cluster),
                null, true);
        return new ClusteredExecutor(pool, () -> pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount());
    }

    public static ExecutorService newFixedThreadPool(int cluster) {
        int nThreads = availableProcessors(cluster);
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(nThreads, nThreads,
            0L, TimeUnit.MILLISECONDS,
            queue,
            defaultThreadFactory(cluster));
        return new ClusteredExecutor(pool, queue::size);
    }

    /**
     * Creates a pool backed by...
     */
    public static ExecutorService newLeastLoadedPool(IntFunction<ExecutorService> factory) {
        List<ClusteredExecutor> pools = IntStream.range(0, availableClusters())
            .mapToObj(cluster -> {
                ExecutorService e = factory.apply(cluster);
                if (!(e instanceof ClusteredExecutor))
                    throw new IllegalArgumentException("ExecutorService must be created by the factory methods of this class");
                return (ClusteredExecutor) e;
            })
            .toList();
        return new LeastLoadedExecutor(pools);
    }

    /**
     * A default thread factory...
     *
     * @see Executors#defaultThreadFactory()
     */
    public static ThreadFactory defaultThreadFactory(int cluster) {
        checkInvariants();
        checkClusterIndex(cluster);
        return wrap(Executors.defaultThreadFactory(), cluster);
    }

    /**
     * A fork join pool worker factory...
     */
    public static ForkJoinPool.ForkJoinWorkerThreadFactory forkJoinWorkerThreadFactory(int cluster) {
        checkInvariants();
        checkClusterIndex(cluster);
        BitSet affinity = clusters().toList().get(cluster);
        return pool -> new ClusteredForkJoinPoolWorkerThread(pool, cluster, affinity);
    }

    /**
     * Returns a wrapped {@link ThreadFactory} ...
     */
    public static ThreadFactory wrap(ThreadFactory factory, int cluster) {
        checkClusterIndex(cluster);
        BitSet affinity = clusters().toList().get(cluster);
        return r -> factory.newThread(() -> {
            currentThreadAffinity(cluster, affinity);
            r.run();
        });
    }

    private static void currentThreadAffinity(int cluster, BitSet affinity) {
        LinuxScheduling.currentThreadAffinity(affinity);
        Thread thread = Thread.currentThread();
        thread.setName(thread.getName() + "-cluster-" + cluster);
    }

    private static void checkInvariants() {
        checkAvailableProcessors();
    }

    private static void checkAvailableProcessors() {
        int availableProcessors = (int) LinuxScheduling.availableProcessors().count();
        if (availableProcessors != initialAvailableProcessors) {
            throw new IllegalStateException("Initital available processors " + initialAvailableProcessors + " differs from current of " + availableProcessors);
        }
    }

    private static void checkClusterIndex(int cluster) {
        Objects.checkIndex(cluster, availableClusters());
    }

    private static Stream<BitSet> clusters() {
        return LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::llcSharedProcessors)
            .map(i -> {
                BitSet cpus = new BitSet();
                i.forEach(cpus::set);
                return cpus;
            }).distinct();
    }

    private static final class ClusteredForkJoinPoolWorkerThread extends ForkJoinWorkerThread {
        private final int cluster;
        private final BitSet affinity;

        private ClusteredForkJoinPoolWorkerThread(ForkJoinPool pool, int cluster, BitSet affinity) {
            super(pool);
            this.cluster = cluster;
            this.affinity = affinity;
        }

        protected void onStart() {
            super.onStart();
            currentThreadAffinity(cluster, affinity);
        }
    }

    /**
     * Wrapper class that only exposes {@link ExecutorService methods, preventing configuration
     * of the service by downstream callers and adding additional private methods supporting the
     * functionality of this class.
     */
    private static class ClusteredExecutor implements ExecutorService {
        private final ExecutorService e;
        private final DoubleSupplier load;

        private ClusteredExecutor(ExecutorService e, DoubleSupplier load) {
            this.e = e;
            this.load = load;
        }

        private ExecutorService delegate() {
            return e;
        }

        private double load() {
            return load.getAsDouble();
        }

        @Override
        public void shutdown() {
            e.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return e.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return e.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return e.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return e.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return e.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return e.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return e.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return e.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return e.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return e.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return e.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            e.execute(command);
        }
    }

    /**
     * Choose two, least loaded.
     */
    private static class LeastLoadedExecutor extends AbstractExecutorService {
        private final List<ClusteredExecutor> pools;
        private final int[][] candidates;

        private LeastLoadedExecutor(List<ClusteredExecutor> pools) {
            if (pools.isEmpty()) throw new IllegalArgumentException("pools can not be empty");
            this.pools = pools;
            this.candidates = IntStream.range(0, pools.size())
                .mapToObj(i -> IntStream.range(0, pools.size())
                    .filter(j -> j != i)
                    .toArray())
                .toArray(int[][]::new);
        }

        @Override
        public void execute(Runnable command) {
            if (pools.size() == 1) {
                pools.getFirst().execute(command);
                return;
            }
            // TODO implement recursive submission for other pools too?
            if (Thread.currentThread() instanceof ClusteredForkJoinPoolWorkerThread t) {
                Optional<ClusteredExecutor> pool = pools.stream().filter(p -> p.delegate() == t.getPool()).findFirst();
                if (pool.isPresent()) {
                    pool.get().execute(command);
                    return;
                }
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int poolIdx = random.nextInt(pools.size());
            int[] neighbours = candidates[poolIdx];
            int candidateIdx = neighbours[random.nextInt(neighbours.length)];
            poolIdx = pools.get(poolIdx).load() > pools.get(candidateIdx).load() ? candidateIdx : poolIdx;
            pools.get(poolIdx).execute(command);
        }

        @Override
        public void shutdown() {
            pools.forEach(ExecutorService::shutdown);
        }

        @Override
        public List<Runnable> shutdownNow() {
            return pools.stream().flatMap(pool -> pool.shutdownNow().stream()).toList();
        }

        @Override
        public boolean isShutdown() {
            return pools.stream().allMatch(ExecutorService::isShutdown);
        }

        @Override
        public boolean isTerminated() {
            return pools.stream().allMatch(ExecutorService::isTerminated);
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long current = System.nanoTime();
            long remaining = unit.toNanos(timeout);
            for (ExecutorService pool : pools) {
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
            return IntStream.range(0, pools.size())
                .mapToObj(i -> i + ": " + pools.get(i))
                .collect(Collectors.joining("\n"));
        }

    }

    private static final int initialAvailableProcessors = Runtime.getRuntime().availableProcessors();

    static {

    }

}
