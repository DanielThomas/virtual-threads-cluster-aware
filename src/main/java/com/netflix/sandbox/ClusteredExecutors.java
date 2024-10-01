package com.netflix.sandbox;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Factory and utility methods providing affinity for threads to improve cache locality and efficiency on processors
 * with clustered last-level caches.
 * <p>
 * The mechanism that provides affinity will differ depending on the capabilities of the underlying operating system,
 * however the following is guarenteed:
 * <ul>
 * <li> Mutual-exclusivity - threads on a given cluster will not run on the same processors as those in another
 * <li> Locality - threads in the same cluster will be scheduled such that they always share a last-level cache
 * </ul>
 * <p>
 * <b>Note</b>: On platforms where thread CPU affinity is used, the result of {@link Runtime#availableProcessors()} may
 * be affected on threads created by this class, depending on how it's implemented on the platform.
 *
 * @author Danny Thomas
 * @since 1.0
 */
public class ClusteredExecutors {

    /**
     * Returns the processor clusters available to the current thread.
     */
    public static List<Cluster> availableClusters() {
        return SCHEDULING.availableClusters();
    }

    /**
     * A default thread factory that provides affinity to the given cluster for created threads.
     *
     * @see Executors#defaultThreadFactory()
     */
    public static ThreadFactory clusteredThreadFactory(Cluster cluster) {
        return new ClusteredThreadFactory(Executors.defaultThreadFactory(), cluster);
    }

    /**
     * Creates a work-stealing thread pool...
     *
     * @throws IllegalArgumentException if the provided thread factory did not originate from one of the supported
     *                                  methods in this class.
     */
    public static ExecutorService newWorkStealingPool(ThreadFactory threadFactory) {
        if (!(threadFactory instanceof ClusteredThreadFactory)) {
            throw new IllegalArgumentException("");
        }
        Cluster cluster = ((ClusteredThreadFactory) threadFactory).cluster;
        return new ForkJoinPool
            (cluster.parallelism(),
                pool -> new ClusteredForkJoinPoolWorkerThread(pool, cluster),
                null, true);
    }

    /**
     * Creates a work-stealing thread pool...
     *
     * @throws IllegalArgumentException if the provided thread factory did not originate from one of the supported
     *                                  methods in this class.
     */
    public static ExecutorService newWorkStealingPool(int parallelism, ThreadFactory threadFactory) {
        if (!(threadFactory instanceof ClusteredThreadFactory)) {
            throw new IllegalArgumentException("");
        }
        Cluster cluster = ((ClusteredThreadFactory) threadFactory).cluster;
        return new ForkJoinPool
            (parallelism,
                pool -> new ClusteredForkJoinPoolWorkerThread(pool, cluster),
                null, true);
    }

    /**
     * Creates a new load balancing {@link ExecutorService}, using the specified strategy to decide how to place load on the
     * pools created by the factory.
     *
     * @param strategy ...`
     * @param factory  ...
     * @return the executor service
     */
    public static ExecutorService newLoadBalancingPool(LoadBalancingStrategy strategy, Function<ThreadFactory, ExecutorService> factory) {
        return switch (strategy) {
            case BIASED -> new BiasedExecutor(factory);
            case CHOOSE_TWO -> new ChooseTwoExecutor(factory);
            case ROUND_ROBIN -> new RoundRobinExecutor(factory);
            case LEAST_LOADED -> throw new UnsupportedOperationException();
        };
    }

    /**
     *
     */
    public static ExecutorService newLoadBalancingPoolWithSize(LoadBalancingStrategy strategy, BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
        return switch (strategy) {
            case BIASED -> new BiasedExecutor(factory);
            case CHOOSE_TWO -> new ChooseTwoExecutor(factory);
            case ROUND_ROBIN -> new RoundRobinExecutor(factory);
            case LEAST_LOADED -> throw new UnsupportedOperationException();
        };
    }

    /**
     * A collection of processors that share a last-level cache.
     */
    public static class Cluster {
        private final int index;
        private final BitSet processors;
        private final int cacheSize;

        private Cluster(int index, BitSet processors, int cacheSize) {
            this.index = index;
            this.processors = processors;
            this.cacheSize = cacheSize;
        }

        /*
         * Returns the index of this cluster.
         */
        public int index() {
            return index;
        }

        /*
         * Return the ideal number of threads for CPU bound workers on this cluster.
         */
        public int parallelism() {
            return processors.cardinality();
        }

        /**
         * Return a {@link BitSet} describing the processors in this cluster.
         */
        public BitSet processors() {
            return (BitSet) processors.clone();
        }

        /**
         * Return the size of the last-level cache for this cluster.
         */
        public OptionalInt cacheSizeInBytes() {
            if (cacheSize <= 0) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(cacheSize);
        }

        public String toString() {
            return processors.toString();
        }
    }

    private interface ProcessorScheduling {
        List<Cluster> availableClusters();

        void constrainCurrentThread(Cluster cluster);
    }

    private static final class UnsupportedProcessorScheduling implements ProcessorScheduling {
        private final Cluster ZERO = new Cluster(0, new BitSet(0), -1);

        @Override
        public List<Cluster> availableClusters() {
            return List.of(ZERO);
        }

        @Override
        public void constrainCurrentThread(Cluster cluster) {
            // do nothing
        }
    }

    private static final class LinuxProcessorScheduling implements ProcessorScheduling {
        @Override
        public List<Cluster> availableClusters() {
            List<BitSet> sharedProcessors = onlineProcessors()
                .mapToObj(this::sharedProcessors)
                .map(i -> {
                    BitSet processors = new BitSet();
                    i.forEach(processors::set);
                    return processors;
                }).distinct()
                .toList();
            return IntStream.range(0, sharedProcessors.size())
                .mapToObj(i -> {
                    BitSet processors = sharedProcessors.get(i);
                    int firstCpu = processors.nextSetBit(0);
                    int cacheSize = sharedCache(firstCpu);
                    return new Cluster(i, processors, cacheSize);
                })
                .toList();
        }

        @Override
        public void constrainCurrentThread(Cluster cluster) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
                long[] longs = cluster.processors.toLongArray();
                Objects.checkIndex(longs.length, CPUSET_SIZE);
                MemorySegment cpu_set = arena.allocateFrom(ValueLayout.JAVA_LONG, longs);
                try {
                    int tid = (int) GETTID.invokeExact();
                    int result = (int) SCHED_SETAFFINITY.invokeExact(capturedState, tid, cpu_set.byteSize(), cpu_set);
                    if (result == -1) {
                        int errno = (int) CAPTURE_STATE.get(capturedState, 0L);
                        throw new RuntimeException("sched_setaffinity failed with errno: " + errno);
                    }
                } catch (Throwable e) {
                    throw wrapChecked(e);
                }
            }
        }

        private IntStream onlineProcessors() {
            Path path = Path.of("/sys/devices/system/cpu/online");
            String onlineCpus;
            try {
                onlineCpus = Files.readString(path).trim();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return parseCpuList(onlineCpus);
        }

        private IntStream sharedProcessors(int cpuId) {
            int cacheLevel = maxCacheLevel(cpuId);
            Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuId, "cache", "index" + cacheLevel, "shared_cpu_list");
            try {
                String sharedCpus = Files.readString(path).trim();
                return parseCpuList(sharedCpus);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public static int maxCacheLevel(int cpuId) {
            Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuId, "cache");
            try (Stream<Path> dirs = Files.list(path).filter(Files::isDirectory)) {
                return dirs.map(Path::getFileName)
                    .map(Path::toString)
                    .filter(filename -> filename.startsWith("index"))
                    .mapToInt(filename -> Integer.parseInt(filename.substring(5)))
                    .max()
                    .getAsInt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public static int sharedCache(int cpuId) {
            int cacheLevel = maxCacheLevel(cpuId);
            Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuId, "cache", "index" + cacheLevel, "size");
            try {
                String size = Files.readString(path).trim();
                String digits = size.chars()
                    .filter(Character::isDigit)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
                String unit = size.substring(digits.length()); // TODO assume K for now
                return Integer.parseInt(digits) * 1024;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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

        // see https://github.com/bminor/glibc/blob/a2509a8bc955988f01f389a1cf74db3a9da42409/posix/bits/cpu-set.h#L27-L29
        private static final int CPUSET_SIZE = 1024 / (Byte.SIZE * (int) ValueLayout.JAVA_LONG.byteSize());

        private static final StructLayout CAPTURE_STATE_LAYOUT;
        private static final VarHandle CAPTURE_STATE;

        private static final MethodHandle GETTID;
        private static final MethodHandle SCHED_SETAFFINITY;

        static {
            Linker linker = Linker.nativeLinker();
            SymbolLookup stdLib = linker.defaultLookup();

            Linker.Option ccs = Linker.Option.captureCallState("errno");
            CAPTURE_STATE_LAYOUT = Linker.Option.captureStateLayout();
            CAPTURE_STATE = CAPTURE_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

            MemorySegment gettid_addr = stdLib.findOrThrow("gettid");
            FunctionDescriptor gettid_sig =
                FunctionDescriptor.of(ValueLayout.JAVA_INT);
            GETTID = linker.downcallHandle(gettid_addr, gettid_sig);

            MemorySegment setaffinity_addr = stdLib.findOrThrow("sched_setaffinity");
            FunctionDescriptor setaffinity_sig =
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);
            SCHED_SETAFFINITY = linker.downcallHandle(setaffinity_addr, setaffinity_sig, ccs);
        }
    }

    private static final ProcessorScheduling SCHEDULING;

    static {
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (osName.contains("linux")) {
            SCHEDULING = new LinuxProcessorScheduling();
        } else {
            // When implementing other platforms, refer to:
            // https://developer.apple.com/library/archive/releasenotes/Performance/RN-AffinityAPI/index.html
            // https://github.com/kimwalisch/primesieve/blob/bf8e09f5c7e1b26a9fd441a777098da55e3fb8c7/src/CpuInfo.cpp#L769
            // https://github.com/Genivia/ugrep/blob/13fa0774dfb3d9b6176fe75bac34b55a4674a268/src/ugrep.cpp#L533
            SCHEDULING = new UnsupportedProcessorScheduling();
        }
    }

    private static RuntimeException wrapChecked(Throwable t) {
        switch (t) {
            case Error e -> throw e;
            case RuntimeException e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }

    private record ClusteredThreadFactory(ThreadFactory threadFactory, Cluster cluster) implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = threadFactory.newThread(() -> {
                SCHEDULING.constrainCurrentThread(cluster);
                r.run();
            });
            thread.setName(thread.getName() + "-cluster-" + cluster.index);
            return thread;
        }
    }

    private static final class ClusteredForkJoinPoolWorkerThread extends ForkJoinWorkerThread {
        private final Cluster cluster;

        private ClusteredForkJoinPoolWorkerThread(ForkJoinPool pool, Cluster cluster) {
            super(pool);
            this.cluster = cluster;
        }

        protected void onStart() {
            super.onStart();
            SCHEDULING.constrainCurrentThread(cluster);
            setName(getName() + "-cluster-" + cluster.index);
        }
    }

    /**
     * Delegate class adding functionality.
     */
    private record ClusteredExecutor(ExecutorService e,
                                     DoubleSupplier loadSupplier,
                                     int[] neighbors) implements ExecutorService {

        private double load() {
            return loadSupplier.getAsDouble();
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
     *
     */
    public enum LoadBalancingStrategy {
        BIASED,
        CHOOSE_TWO,
        ROUND_ROBIN,
        LEAST_LOADED
    }

    private static abstract class LoadBalancingExecutor extends AbstractExecutorService {
        private final List<ClusteredExecutor> pools;

        private LoadBalancingExecutor(Function<ThreadFactory, ExecutorService> factory) {
            this(availableClusters()
                .stream()
                .map(ClusteredExecutors::clusteredThreadFactory)
                .map(factory)
                .toList());
        }

        private LoadBalancingExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            this(availableClusters()
                .stream()
                .map(ClusteredExecutors::clusteredThreadFactory)
                .map(threadFactory -> {
                    Cluster cluster = ((ClusteredThreadFactory) threadFactory).cluster;
                    return factory.apply(cluster.parallelism(), threadFactory);
                })
                .toList());
        }

        private LoadBalancingExecutor(List<ExecutorService> pools) {
            this.pools = IntStream.range(0, pools.size())
                .mapToObj(i -> {
                    ExecutorService executor = pools.get(i);
                    DoubleSupplier loadSupplier = loadFunction(executor);
                    int[] neighbors = IntStream.range(0, pools.size())
                        .filter(j -> j != i)
                        .toArray();
                    return new ClusteredExecutor(executor, loadSupplier, neighbors);
                }).toList();
        }

        private DoubleSupplier loadFunction(ExecutorService executor) {
            return switch (executor) {
                case ThreadPoolExecutor pool -> () -> {
                    long taskCount = pool.getActiveCount() + (pool.getCompletedTaskCount() - pool.getTaskCount());
                    return taskCount == 0 ? 0 : (double) taskCount / pool.getPoolSize();
                };
                case ForkJoinPool pool -> () -> {
                    long taskCount = pool.getActiveThreadCount() + pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount();
                    return taskCount == 0 ? 0 : (double) taskCount / pool.getParallelism();
                };
                default ->
                    throw new IllegalArgumentException(executor.getClass() + " is not a supported ExecutorService implementation");
            };
        }

        protected abstract ClusteredExecutor choosePool(List<ClusteredExecutor> pools);

        @Override
        public void execute(Runnable command) {
            if (pools.size() == 1) {
                pools.getFirst().execute(command);
                return;
            }
            // TODO should we pin recursive executions by default?
            // TODO that decision needs to consider if this executor 'owns' the thread
            if (Thread.currentThread() instanceof ClusteredForkJoinPoolWorkerThread t) {
                t.getPool().execute(command);
                return;
            }
            choosePool(pools).execute(command);
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

    private static final class ChooseTwoExecutor extends LoadBalancingExecutor {
        private ChooseTwoExecutor(Function<ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        private ChooseTwoExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        @Override
        protected ClusteredExecutor choosePool(List<ClusteredExecutor> pools) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int index = random.nextInt(pools.size());
            ClusteredExecutor pool = pools.get(index);
            int alternateIndex = pool.neighbors[random.nextInt(pool.neighbors.length)];
            ClusteredExecutor alternate = pools.get(alternateIndex);
            return pool.load() < alternate.load() ? pool : alternate;
        }
    }

    private static final class RoundRobinExecutor extends LoadBalancingExecutor {
        private final AtomicInteger counter = new AtomicInteger();

        private RoundRobinExecutor(Function<ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        private RoundRobinExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        @Override
        protected ClusteredExecutor choosePool(List<ClusteredExecutor> pools) {
            int index = counter.accumulateAndGet(1, (i, _) -> ++i >= pools.size() ? 0 : i);
            return pools.get(index);
        }
    }

    private static final class BiasedExecutor extends LoadBalancingExecutor {
        private BiasedExecutor(Function<ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        private BiasedExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        @Override
        protected ClusteredExecutor choosePool(List<ClusteredExecutor> pools) {
            Optional<ClusteredExecutor> pool = pools.stream()
                .filter(p -> p.load() < 1.0)
                .findFirst();
            Optional<ClusteredExecutor> leastLoaded = pools.stream()
                .min(Comparator.comparingDouble(ClusteredExecutor::load));
            return pool.orElse(leastLoaded.get());
        }
    }

    private ClusteredExecutors() {
        // static utility class
    }
}
