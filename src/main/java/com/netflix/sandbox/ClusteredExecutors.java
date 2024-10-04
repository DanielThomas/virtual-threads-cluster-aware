package com.netflix.sandbox;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
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
     * Returns the processor clusters available.
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
        return new ClusteredThreadFactory(cluster);
    }

    /**
     * Creates a work-stealing thread pool. Convenience method allowing static function references to be used with the
     * methods in this class that accept an argument with the desired parallelism and a {@link ThreadFactory}.
     *
     * @throws IllegalArgumentException if the provided thread factory did not originate from one of the supported
     *                                  methods in this class.
     */
    public static ExecutorService newWorkStealingPool(int parallelism, ThreadFactory threadFactory) {
        if (!(threadFactory instanceof ClusteredThreadFactory)) {
            throw new IllegalArgumentException("These methods are intended for use only with ThreadFactory instances provided by this class");
        }
        Cluster cluster = ((ClusteredThreadFactory) threadFactory).cluster;
        return new ForkJoinPool
            (parallelism,
                pool -> new ClusteredForkJoinPoolWorkerThread(pool, cluster),
                null, true);
    }

    /**
     * Creates a new {@link ExecutorService} backed by an separate executor per processor cluster, using the provided
     * placement strategy to decide how to place tasks on the underlying threads.
     *
     *
     *
     * @param strategy the placement strategy for determining which underlying pool is selected for execution
     * @param factory  ...
     * @return the executor service
     */
    public static ExecutorService newClusteredPool(PlacementStrategy strategy, BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
        return switch (strategy) {
            case BIASED -> new BiasedExecutor(factory);
            case CHOOSE_TWO -> new ChooseTwoExecutor(factory);
            case CURRENT -> new CurrentClusterExecutor(factory);
            case ROUND_ROBIN -> new RoundRobinExecutor(factory);
        };
    }

    /**
     * Creates a new {@link ExecutorService} backed by an separate executor per processor cluster.
     *
     * @param strategy the placement strategy for determining which underlying pool is selected for execution
     * @param factory  ...
     * @return the executor service
     */
    public static ExecutorService newClusteredPoolWithoutSize(PlacementStrategy strategy, Function<ThreadFactory, ExecutorService> factory) {
        return switch (strategy) {
            case BIASED -> new BiasedExecutor(factory);
            case CHOOSE_TWO -> new ChooseTwoExecutor(factory);
            case CURRENT -> new CurrentClusterExecutor(factory);
            case ROUND_ROBIN -> new RoundRobinExecutor(factory);
        };
    }

    /**
     * A collection of processors that share a last-level cache.
     */
    public static class Cluster {
        private final int index;
        private final BitSet processors;
        private final long lastLevelCacheSize;

        private Cluster(int index, BitSet processors, long lastLevelCacheSize) {
            this.index = index;
            this.processors = processors;
            this.lastLevelCacheSize = lastLevelCacheSize;
        }

        /*
         * Returns the index of this cluster.
         */
        public int index() {
            return index;
        }

        /*
         * Return the number of available processors in this cluster.
         */
        public int availableProcessors() {
            return processors.cardinality();
        }

        /**
         * Return the size of the last-level cache for this cluster in bytes.
         */
        public OptionalLong lastLevelCacheSize() {
            if (lastLevelCacheSize <= 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(lastLevelCacheSize);
        }

        public String toString() {
            return index + ": " + processors.toString();
        }
    }

    private interface ProcessorScheduling {
        List<Cluster> availableClusters();

        Cluster currentCluster();

        void constrainCurrentThread(Cluster cluster);
    }

    private static final class UnsupportedProcessorScheduling implements ProcessorScheduling {
        private final Cluster ZERO = new Cluster(0, new BitSet(0), -1);

        @Override
        public List<Cluster> availableClusters() {
            return List.of(ZERO);
        }

        @Override
        public Cluster currentCluster() {
            return ZERO;
        }

        @Override
        public void constrainCurrentThread(Cluster cluster) {
            // do nothing
        }
    }

    private static abstract class AbstractProcessorScheduling implements ProcessorScheduling {
        private final List<Cluster> availableClusters;
        private final List<Cluster> clusterByProcessor;

        protected AbstractProcessorScheduling() {
            Thread t = Thread.currentThread();
            if (t instanceof ClusteredThread || t instanceof ClusteredForkJoinPoolWorkerThread) {
                throw new IllegalStateException("Should not be called from clustered threads");
            }
            BitSet availableProcessors = availableProcessors();
            List<BitSet> sharedProcessors = availableProcessors
                .stream()
                .mapToObj(this::lastLevelCacheSharedProcessors)
                .map(i -> {
                    BitSet processors = new BitSet();
                    i.forEach(processors::set);
                    return processors;
                }).distinct()
                .toList();
            availableClusters = IntStream.range(0, sharedProcessors.size())
                .mapToObj(i -> {
                    BitSet processors = sharedProcessors.get(i);
                    int firstCpuId = processors.nextSetBit(0);
                    long cacheSize = lastLevelCacheSize(firstCpuId);
                    return new Cluster(i, processors, cacheSize);
                })
                .toList();
            clusterByProcessor = new ArrayList<>(availableProcessors.stream().max().getAsInt());
            availableClusters.forEach(cluster ->
                cluster.processors.stream().forEach(i -> clusterByProcessor.add(i, cluster))
            );
        }

        @Override
        public final List<Cluster> availableClusters() {
            return availableClusters;
        }

        @Override
        public final Cluster currentCluster() {
            return switch (Thread.currentThread()) {
                case ClusteredThread t -> t.cluster;
                case ClusteredForkJoinPoolWorkerThread t -> t.cluster;
                default -> {
                    int cpuId = currentProcessor();
                    yield clusterByProcessor.get(cpuId);
                }
            };
        }

        protected abstract BitSet availableProcessors();

        protected abstract int currentProcessor();

        protected abstract IntStream lastLevelCacheSharedProcessors(int cpuId);

        protected abstract long lastLevelCacheSize(int cpuId);
    }

    private static final class LinuxProcessorScheduling extends AbstractProcessorScheduling {
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

        protected int currentProcessor() {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
                int result = (int) SCHED_GETCPU.invokeExact(capturedState);
                if (result == -1) {
                    int errno = (int) CAPTURE_STATE.get(capturedState, 0L);
                    throw new RuntimeException("sched_getcpu failed with errno: " + errno);
                }
                return result;
            } catch (Throwable e) {
                throw wrapChecked(e);
            }
        }

        protected BitSet availableProcessors() {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
                MemorySegment cpu_set = arena.allocate(CPUSET_BYTE_SIZE);
                try {
                    int tid = (int) GETTID.invokeExact();
                    int result = (int) SCHED_GETAFFINITY.invokeExact(capturedState, tid, cpu_set.byteSize(), cpu_set);
                    if (result == -1) {
                        int errno = (int) CAPTURE_STATE.get(capturedState, 0L);
                        throw new RuntimeException("sched_getaffinity failed with errno: " + errno);
                    }
                } catch (Throwable e) {
                    throw wrapChecked(e);
                }
                long[] longs = cpu_set.toArray(ValueLayout.JAVA_LONG);
                return BitSet.valueOf(longs);
            }
        }

        protected IntStream lastLevelCacheSharedProcessors(int cpuId) {
            int cacheLevel = maxCacheLevel(cpuId);
            Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuId, "cache", "index" + cacheLevel, "shared_cpu_list");
            String sharedCpus;
            try {
                sharedCpus = Files.readString(path).trim();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return parseCpuList(sharedCpus);
        }

        public int maxCacheLevel(int cpuId) {
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

        public long lastLevelCacheSize(int cpuId) {
            int cacheLevel = maxCacheLevel(cpuId);
            Path path = Path.of("/sys/devices/system/cpu/cpu" + cpuId, "cache", "index" + cacheLevel, "size");
            try {
                String size = Files.readString(path).trim();
                String digits = size.chars()
                    .filter(Character::isDigit)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
                // TODO assume K for now
                return Long.parseLong(digits) * 1024;
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
        private static final int CPUSET_BYTE_SIZE = 1024 / Byte.SIZE;

        private static final StructLayout CAPTURE_STATE_LAYOUT;
        private static final VarHandle CAPTURE_STATE;

        private static final MethodHandle GETTID;
        private static final MethodHandle SCHED_GETCPU;
        private static final MethodHandle SCHED_SETAFFINITY;
        private static final MethodHandle SCHED_GETAFFINITY;

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

            MemorySegment getcpu_addr = stdLib.findOrThrow("sched_getcpu");
            FunctionDescriptor getcpu_sig =
                FunctionDescriptor.of(ValueLayout.JAVA_INT);
            SCHED_GETCPU = linker.downcallHandle(getcpu_addr, getcpu_sig, ccs);

            MemorySegment getaffinity_addr = stdLib.findOrThrow("sched_getaffinity");
            FunctionDescriptor getaffinity_sig =
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);
            SCHED_GETAFFINITY = linker.downcallHandle(getaffinity_addr, getaffinity_sig, ccs);

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
            System.err.println("WARNING: ClusteredExecutor does not support this platform and will fall-back to all processors allowed");
        }
    }

    private static RuntimeException wrapChecked(Throwable t) {
        switch (t) {
            case Error e -> throw e;
            case RuntimeException e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }

    /**
     * Thread factory creating {@link ClusteredThread}s, but otherwise having the same behaviour as
     * {@link Executors#defaultThreadFactory()}.
     */
    private static class ClusteredThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final Cluster cluster;
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final String nameSuffix;

        ClusteredThreadFactory(Cluster cluster) {
            this.cluster = cluster;
            group = Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" +
                poolNumber.getAndIncrement() +
                "-thread-";
            nameSuffix = "-cluster-" + cluster.index;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new ClusteredThread(group, r, namePrefix + threadNumber.getAndIncrement() + nameSuffix, cluster);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    private static final class ClusteredThread extends Thread {
        private final Cluster cluster;

        private ClusteredThread(ThreadGroup group, Runnable task, String name, Cluster cluster) {
            super(group, task, name);
            this.cluster = cluster;
        }

        @Override
        public void run() {
            SCHEDULING.constrainCurrentThread(cluster);
            super.run();
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
    public enum PlacementStrategy {
        CURRENT,
        BIASED,
        CHOOSE_TWO,
        ROUND_ROBIN
    }

    private static abstract class ClusterPlacementExecutor extends AbstractExecutorService {
        private final List<ClusteredExecutor> pools;

        private ClusterPlacementExecutor(Function<ThreadFactory, ExecutorService> factory) {
            this(availableClusters()
                .stream()
                .map(ClusteredExecutors::clusteredThreadFactory)
                .map(factory)
                .toList());
        }

        private ClusterPlacementExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            this(availableClusters()
                .stream()
                .map(ClusteredExecutors::clusteredThreadFactory)
                .map(threadFactory -> {
                    Cluster cluster = ((ClusteredThreadFactory) threadFactory).cluster;
                    return factory.apply(cluster.availableProcessors(), threadFactory);
                })
                .toList());
        }

        private ClusterPlacementExecutor(List<ExecutorService> pools) {
            this.pools = IntStream.range(0, pools.size())
                .mapToObj(i -> {
                    ExecutorService executor = pools.get(i);
                    DoubleSupplier loadSupplier = loadSupplier(executor);
                    int[] neighbors = IntStream.range(0, pools.size())
                        .filter(j -> j != i)
                        .toArray();
                    return new ClusteredExecutor(executor, loadSupplier, neighbors);
                }).toList();
        }

        private DoubleSupplier loadSupplier(ExecutorService executor) {
            return switch (executor) {
                case ThreadPoolExecutor pool -> () -> {
                    long taskCount = pool.getActiveCount() + pool.getQueue().size();
                    return taskCount == 0 ? 0 : (double) taskCount / Math.max(pool.getCorePoolSize(), pool.getPoolSize());
                };
                case ForkJoinPool pool -> () -> {
                    long taskCount = pool.getActiveThreadCount() + pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount();
                    return taskCount == 0 ? 0 : (double) taskCount / Math.max(pool.getParallelism(), pool.getActiveThreadCount());
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

    private static final class CurrentClusterExecutor extends ClusterPlacementExecutor {
        private CurrentClusterExecutor(Function<ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        private CurrentClusterExecutor(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            super(factory);
        }

        @Override
        protected ClusteredExecutor choosePool(List<ClusteredExecutor> pools) {
            Cluster cluster = switch (Thread.currentThread()) {
                case ClusteredThread t -> t.cluster;
                case ClusteredForkJoinPoolWorkerThread t -> t.cluster;
                default -> SCHEDULING.currentCluster();
            };
            return pools.get(cluster.index);
        }
    }

    private static final class ChooseTwoExecutor extends ClusterPlacementExecutor {
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

    private static final class RoundRobinExecutor extends ClusterPlacementExecutor {
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

    private static final class BiasedExecutor extends ClusterPlacementExecutor {
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
