package com.netflix.sandbox;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
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
 */
public class ClusteredExecutors {

    /**
     * Returns the processor clusters available.
     */
    public static List<Cluster> availableClusters() {
        return SCHEDULING.availableClusters();
    }

    /**
     * A default thread factory that provides affinity to the given {@link Cluster} for created threads.
     *
     * @see Executors#defaultThreadFactory()
     */
    public static ThreadFactory clusteredThreadFactory(Cluster cluster) {
        return new ClusteredThreadFactory(cluster);
    }

    /**
     * Creates a work-stealing thread pool...
     */
    public static ExecutorService newWorkStealingPool() {
        return newWorkStealingPool(PlacementStrategy.CHOOSE_TWO);
    }

    /**
     * Creates a work-stealing thread pool...
     */
    public static ExecutorService newWorkStealingPool(PlacementStrategy strategy) {
        return newWorkStealingPool(strategy, new ServiceLevel());
    }

    /**
     * Creates a work-stealing thread pool...
     */
    public static ExecutorService newWorkStealingPool(PlacementStrategy strategy, ServiceLevel serviceLevel) {
        int clusteredPoolId = POOL_IDS.incrementAndGet();
        BiFunction<Integer, ThreadFactory, ExecutorService> factory = (parallelism, threadFactory) ->
            new ClusteredForkJoinPool(parallelism, ((ClusteredThreadFactory) threadFactory).cluster, clusteredPoolId);
        return new ClusterPlacementExecutor(clusteredPoolId, strategy, serviceLevel, factory);
    }

    /**
     * Creates a new {@link ExecutorService} backed by an separate executor per processor cluster, using the
     * {@link PlacementStrategy#CHOOSE_TWO} placement strategy.
     *
     * @param factory a factory method to create executors for each cluster, for instance {@link Executors#newFixedThreadPool(int, ThreadFactory)}
     * @return the resulting executor service
     */
    public static ExecutorService newThreadPool(BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
        return newThreadPool(PlacementStrategy.CHOOSE_TWO, factory);
    }

    /**
     * Creates a new {@link ExecutorService} backed by an separate executor per processor cluster, using the provided
     * placement strategy to decide how to place tasks on the underlying pools.
     *
     * @param strategy the placement strategy for determining which underlying pool is selected for execution
     * @param factory  a factory method to create executors for each cluster, for instance {@link Executors#newFixedThreadPool(int, ThreadFactory)}
     * @return the resulting executor service
     */
    public static ExecutorService newThreadPool(PlacementStrategy strategy, BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
        return new ClusterPlacementExecutor(POOL_IDS.incrementAndGet(), strategy, new ServiceLevel(), factory);
    }

    /**
     * Creates a new {@link ExecutorService} backed by an separate pools per processor cluster, using the provided
     * placement strategy to decide how to place tasks on the underlying threads.
     *
     * @param strategy the placement strategy for determining which underlying pool is selected for execution
     * @param factory  a factory method to create executors for each cluster, for instance {@link Executors#newFixedThreadPool(int, ThreadFactory)}
     * @return the resulting executor service
     */
    public static ExecutorService newThreadPoolWithoutSize(PlacementStrategy strategy, Function<ThreadFactory, ExecutorService> factory) {
        return new ClusterPlacementExecutor(POOL_IDS.incrementAndGet(), strategy, new ServiceLevel(), factory);
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

    private interface Clustered {
        Cluster cluster();

        OptionalInt clusteredPoolId();
    }

    /**
     * Thread factory creating {@link ClusteredThread}s with the same behaviour as {@link Executors#defaultThreadFactory()}.
     */
    private static class ClusteredThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final Cluster cluster;
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final String nameSuffix;
        private final int clusteredPoolId;

        ClusteredThreadFactory(Cluster cluster) {
            this(cluster, -1);
        }

        ClusteredThreadFactory(Cluster cluster, int clusteredPoolId) {
            this.cluster = cluster;
            this.clusteredPoolId = clusteredPoolId;
            group = Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" +
                poolNumber.getAndIncrement() +
                "-thread-";
            nameSuffix = "-cluster-" + cluster.index;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new ClusteredThread(group, r, namePrefix + threadNumber.getAndIncrement() + nameSuffix, cluster, clusteredPoolId);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    private static final class ClusteredThread extends Thread implements Clustered {
        private final Cluster cluster;
        private final int clusteredPoolId;

        private ClusteredThread(ThreadGroup group, Runnable task, String name, Cluster cluster, int clusteredPoolId) {
            super(group, task, name);
            this.cluster = cluster;
            this.clusteredPoolId = clusteredPoolId;
        }

        @Override
        public void run() {
            SCHEDULING.constrainCurrentThread(cluster);
            super.run();
        }

        @Override
        public Cluster cluster() {
            return cluster;
        }

        @Override
        public OptionalInt clusteredPoolId() {
            if (clusteredPoolId <= 0) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(clusteredPoolId);
        }
    }

    private static final class ClusteredForkJoinPoolWorkerThread extends ForkJoinWorkerThread implements Clustered {
        private final Cluster cluster;
        private final int clusteredPoolId;

        private ClusteredForkJoinPoolWorkerThread(ForkJoinPool pool, Cluster cluster, int clusteredPoolId) {
            super(pool);
            this.cluster = cluster;
            this.clusteredPoolId = clusteredPoolId;
        }

        protected void onStart() {
            super.onStart();
            SCHEDULING.constrainCurrentThread(cluster);
            setName(getName() + "-cluster-" + cluster.index);
        }

        @Override
        public Cluster cluster() {
            return cluster;
        }

        @Override
        public OptionalInt clusteredPoolId() {
            if (clusteredPoolId <= 0) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(clusteredPoolId);
        }
    }

    private static final class ClusteredForkJoinPool extends ForkJoinPool implements Clustered {
        private final Cluster cluster;
        private final int clusteredPoolId;

        private ClusteredForkJoinPool(int parallelism, Cluster cluster, int clusteredPoolId) {
            super(parallelism,
                pool -> new ClusteredForkJoinPoolWorkerThread(pool, cluster, clusteredPoolId),
                null, true);
            this.cluster = cluster;
            this.clusteredPoolId = clusteredPoolId;
        }

        public void execute(Runnable task) {
            externalSubmit(ForkJoinTask.adapt(task));
        }

        @Override
        public Cluster cluster() {
            return cluster;
        }

        @Override
        public OptionalInt clusteredPoolId() {
            if (clusteredPoolId <= 0) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(clusteredPoolId);
        }
    }

    /**
     * TODO
     */
    public record ServiceLevel(Duration waitThreshold,
                               Duration rebalanceThreshold) {
        public ServiceLevel() {
            this(Duration.ZERO, Duration.ZERO);
        }
    }

    /**
     * TODO
     */
    public enum PlacementStrategy implements ClusterPlacement {
        /**
         * TODO
         */
        BIASED {
            @Override
            public int choose(int[] candidateIndexes,
                              IntToDoubleFunction loadFunction,
                              ConcurrentMap<String, Object> state) {
                return candidateIndexes[0];
            }
        },
        /**
         * TODO
         */
        CURRENT {
            @Override
            public int choose(int[] candidateIndexes,
                              IntToDoubleFunction loadFunction,
                              ConcurrentMap<String, Object> state) {
                Cluster cluster = switch (Thread.currentThread()) {
                    case ClusteredThread t -> t.cluster;
                    case ClusteredForkJoinPoolWorkerThread t -> t.cluster;
                    default -> SCHEDULING.currentCluster();
                };
                return cluster.index;
            }
        },
        /**
         * TODO
         */
        CHOOSE_TWO {
            @Override
            public int choose(int[] candidateIndexes,
                              IntToDoubleFunction loadFunction,
                              ConcurrentMap<String, Object> state) {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                int index = random.nextInt(candidateIndexes.length);
                int[] neighbors = IntStream.of(candidateIndexes)
                    .filter(i -> i != index)
                    .toArray();
                int alternate = neighbors[random.nextInt(neighbors.length)];
                return loadFunction.applyAsDouble(index) < loadFunction.applyAsDouble(alternate) ? index : alternate;
            }
        },
        /**
         * TODO
         */
        ROUND_ROBIN {
            @Override
            public int choose(int[] candidateIndexes,
                              IntToDoubleFunction loadFunction,
                              ConcurrentMap<String, Object> state) {
                AtomicInteger counter = (AtomicInteger) state.computeIfAbsent("ROUND_ROBIN_COUNTER", _ -> new AtomicInteger());
                int count = counter.accumulateAndGet(1, (i, _) -> ++i >= candidateIndexes.length ? 0 : i);
                return candidateIndexes[count];
            }
        }
    }

    private interface ClusterPlacement {
        int choose(int[] candidateClusters,
                   IntToDoubleFunction loadFunction,
                   ConcurrentMap<String, Object> state);
    }

    private static final class ClusterPlacementExecutor extends AbstractExecutorService {
        private final int poolId;
        private final PlacementStrategy strategy;
        private final ServiceLevel serviceLevel;
        private final List<ExecutorService> pools;
        private final int[] poolIndexes;
        private final ConcurrentMap<String, Object> strategyState;
        private final List<AtomicLong> lastWaitTime;
        private final ToLongFunction<ExecutorService> queuedTasksFunction;
        private final ToIntFunction<ExecutorService> poolSizeFunction;
        private final ToIntBiFunction<ExecutorService, Integer> availableThreadsFunction;

        private ClusterPlacementExecutor(int poolId,
                                         PlacementStrategy strategy,
                                         ServiceLevel serviceLevel,
                                         Function<ThreadFactory, ExecutorService> factory) {
            this(poolId, strategy, serviceLevel, availableClusters()
                .stream()
                .map(cluster -> new ClusteredThreadFactory(cluster, poolId))
                .map(factory)
                .toList());
        }

        private ClusterPlacementExecutor(int poolId,
                                         PlacementStrategy strategy,
                                         ServiceLevel serviceLevel,
                                         BiFunction<Integer, ThreadFactory, ExecutorService> factory) {
            this(poolId, strategy, serviceLevel, availableClusters()
                .stream()
                .map(cluster -> new ClusteredThreadFactory(cluster, poolId))
                .map(threadFactory -> {
                    Cluster cluster = threadFactory.cluster;
                    return factory.apply(cluster.availableProcessors(), threadFactory);
                })
                .toList());
        }

        private ClusterPlacementExecutor(int poolId,
                                         PlacementStrategy strategy,
                                         ServiceLevel serviceLevel,
                                         List<ExecutorService> pools) {
            this.poolId = poolId;
            this.strategy = Objects.requireNonNull(strategy);
            this.serviceLevel = Objects.requireNonNull(serviceLevel);
            this.pools = Objects.requireNonNull(pools);
            this.poolIndexes = IntStream.range(0, pools.size()).toArray();
            this.strategyState = new ConcurrentHashMap<>();
            this.lastWaitTime = pools.stream().map(_ -> new AtomicLong()).toList();
            ExecutorService first = pools.stream().findFirst().get();
            this.queuedTasksFunction = queuedTasksFunction(first);
            this.poolSizeFunction = poolSizeFunction(first);
            this.availableThreadsFunction = availableThreadsFunction(first);
        }

        @Override
        public void execute(Runnable command) {
            if (pools.size() == 1) {
                pools.getFirst().execute(command);
                return;
            }
            int index = -1;
            if (Thread.currentThread() instanceof Clustered t) {
                OptionalInt poolId = t.clusteredPoolId();
                if (poolId.isPresent() && this.poolId == poolId.getAsInt()) {
                    int current = t.cluster().index;
                    if (meetsServiceLevel(current)) {
                        index = current;
                    }
                }
            }
            if (index < 0) {
                int[] candidates = IntStream.of(poolIndexes)
                    .filter(this::meetsServiceLevel)
                    .toArray();
                if (candidates.length == 1) {
                    index = candidates[0];
                } else {
                    if (candidates.length == 0) {
                        candidates = poolIndexes;
                    }
                    index = strategy.choose(candidates, i -> {
                        ExecutorService pool = pools.get(i);
                        int poolSize = poolSizeFunction.applyAsInt(pool);
                        return (double) queuedTasksFunction.applyAsLong(pool) / poolSize;
                    }, strategyState);
                }
            }
            Runnable task = command;
            if (!serviceLevel.waitThreshold().isPositive()) {
                AtomicLong lastWaitTime = this.lastWaitTime.get(index);
                long arrivalTime = System.nanoTime();
                task = () -> {
                    lastWaitTime.set(System.nanoTime() - arrivalTime);
                    command.run();
                };
            }
            pools.get(index).execute(task);
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

        private ToLongFunction<ExecutorService> queuedTasksFunction(ExecutorService first) {
            return switch (first) {
                case ThreadPoolExecutor _ -> executor -> {
                    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
                    return pool.getQueue().size();
                };
                case ClusteredForkJoinPool _ -> executor -> {
                    ClusteredForkJoinPool pool = (ClusteredForkJoinPool) executor;
                    return pool.getQueuedSubmissionCount();
                };
                case ForkJoinPool _ -> executor -> {
                    ClusteredForkJoinPool pool = (ClusteredForkJoinPool) executor;
                    return pool.getQueuedSubmissionCount() + pool.getQueuedTaskCount();
                };
                default ->
                    throw new IllegalArgumentException(first.getClass() + " is not supported. Must be a ThreadPoolExecutor or ForkJoinPool");
            };
        }

        private ToIntFunction<ExecutorService> poolSizeFunction(ExecutorService first) {
            return switch (first) {
                case ThreadPoolExecutor _ -> executor -> {
                    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
                    return Math.max(pool.getCorePoolSize(), pool.getPoolSize());
                };
                case ForkJoinPool _ -> executor -> {
                    ClusteredForkJoinPool pool = (ClusteredForkJoinPool) executor;
                    return Math.max(pool.getParallelism(), pool.getPoolSize());
                };
                default ->
                    throw new IllegalArgumentException(first.getClass() + " is not supported. Must be a ThreadPoolExecutor or ForkJoinPool");
            };
        }

        private ToIntBiFunction<ExecutorService, Integer> availableThreadsFunction(ExecutorService first) {
            return switch (first) {
                case ThreadPoolExecutor _ -> (executor, poolSize) -> {
                    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
                    return poolSize - pool.getActiveCount();
                };
                case ForkJoinPool _ -> (executor, poolSize) -> {
                    ClusteredForkJoinPool pool = (ClusteredForkJoinPool) executor;
                    return poolSize - pool.getActiveThreadCount();
                };
                default ->
                    throw new IllegalArgumentException(first.getClass() + " is not supported. Must be a ThreadPoolExecutor or ForkJoinPool");
            };
        }


        private boolean meetsServiceLevel(int index) {
            Duration latencyGoal = serviceLevel.waitThreshold();
            if (!latencyGoal.isPositive()) {
                ExecutorService pool = pools.get(index);
                int poolSize = poolSizeFunction.applyAsInt(pool);
                int availableThreads = availableThreadsFunction.applyAsInt(pool, poolSize);
                return availableThreads > 0 || lastWaitTime.get(index).get() < latencyGoal.toNanos();
            }
            return true;
        }
    }

    interface ProcessorScheduling {
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

    static final class LinuxProcessorScheduling extends AbstractProcessorScheduling {
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
    private static final AtomicInteger POOL_IDS = new AtomicInteger();

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

    private ClusteredExecutors() {
        // static utility class
    }
}
