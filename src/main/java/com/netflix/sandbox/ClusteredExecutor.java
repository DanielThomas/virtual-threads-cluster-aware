package com.netflix.sandbox;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * An {@link ExecutorService} ...
 */
public class ClusteredExecutor extends AbstractExecutorService {

    private static final AtomicLong EXECUTOR_ID = new AtomicLong();
    private static final int LOCAL_QUEUE_SIZE = 256; // FIXME this should be a factor * number of CPUs in a domain

    private final String name;
    private final ThreadPoolExecutor executor;
    private final RunQueue queue;

    /**
     * Creates a {@link ClusteredExecutor} with parallelism and clusters
     * automatically determined the currently available processors and the highest level cache
     * shared between those processors.
     */
    public ClusteredExecutor() {
        BitSet[] domainAffinity = LinuxScheduling.availableProcessors()
            .mapToObj(LinuxScheduling::llcSharedProcessors).map(cpus -> {
                BitSet bs = new BitSet();
                cpus.forEach(bs::set);
                return bs;
            }).distinct()
            .toArray(BitSet[]::new);
        BitSet availableProcessors = Arrays.stream(domainAffinity).reduce(new BitSet(), (a, b) -> {
            a.or(b);
            return a;
        });
        int[] domains = new int[availableProcessors.length()];
        Arrays.fill(domains, -1);
        for (int i = 0; i < domainAffinity.length; i++) {
            int domain = i;
            domainAffinity[i].stream().forEach(processor -> {
                if (domains[processor] != -1)
                    throw new IllegalArgumentException("processor %d is in both domain %d and %d".formatted(processor, domain, domains[processor]));
                domains[processor] = domain;
            });
        }

        int corePoolSize = availableProcessors.cardinality();
        int[] processors = new int[corePoolSize];
        int fromIndex = 0;
        for (int i = 0; i < corePoolSize; i++) {
            int next = availableProcessors.nextSetBit(fromIndex);
            processors[i] = next;
            fromIndex = next + 1;
        }
        
        long id = EXECUTOR_ID.incrementAndGet();
        name = "ClusteredExecutor-" + id;
        AtomicInteger wid = new AtomicInteger();
        this.queue = new RunQueue(this, domainAffinity.length);
        ThreadGroup group = new ThreadGroup(name);
        ThreadFactory factory = task -> {
            int i = wid.getAndIncrement();
            int processor = processors[i];
            int domain = domains[processor];
            BitSet affinity = domainAffinity[domain];
            Runnable r = () -> {
                LinuxScheduling.currentThreadAffinity(affinity);
                task.run();
            };
            return new Worker(group, r, "%s-%d-%d".formatted(name, domain, processor), this, domain);
        };
        this.executor = new ThreadPoolExecutor(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS, queue, factory);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public String toString() {
        return executor.toString();
    }

    /**
     * Skeleton implementation for the methods reachable by our use of {@link ThreadPoolExecutor}.
     */
    private static class RunQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {
        private static final int SPIN_WAITS = 128;

        private final ClusteredExecutor owner;
        private final Lock lock = new ReentrantLock();
        private final Queue<Runnable> external;
        private final Queue<Runnable>[] local;
        private final Condition[] notEmpty;

        private RunQueue(ClusteredExecutor owner, int numDomains) {
            this.owner = owner;
            this.external = new LinkedList<>();
            this.local = IntStream.range(0, numDomains)
                .mapToObj(_ -> new ArrayDeque<Runnable>(LOCAL_QUEUE_SIZE))
                .toArray(ArrayDeque[]::new);
            this.notEmpty = IntStream.range(0, numDomains)
                .mapToObj(_ -> lock.newCondition())
                .toArray(Condition[]::new);
        }

        @Override
        public int size() {
            return external.size() + Arrays.stream(local).mapToInt(Queue::size).sum();
        }

        @Override
        public boolean offer(Runnable runnable) {
            lock.lock();
            try {
                if (Thread.currentThread() instanceof Worker w && w.owner == owner) {
                    if (local[w.domain].offer(runnable)) {
                        notEmpty[w.domain].signal();
                        return true;
                    }
                }
                external.offer(runnable);
                notEmpty[ThreadLocalRandom.current().nextInt(local.length)].signal();
                return true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Runnable take() throws InterruptedException {
            Worker w = (Worker) Thread.currentThread();
            lock.lock();
            try {
                for (int waits = 0; ; ) {
                    if (waits < SPIN_WAITS) {
                        Runnable r = local[w.domain].poll();
                        r = r == null ? external.poll() : r;
                        if (r != null)
                            return r;
                        if (w.isInterrupted())
                            throw new InterruptedException();
                        ++waits;
                        Thread.onSpinWait();
                    } else {
                        notEmpty[w.domain].await();
                        waits = 0;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int remainingCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public Iterator<Runnable> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Runnable peek() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Runnable poll() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(Runnable runnable) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection<? super Runnable> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection<? super Runnable> c, int maxElements) {
            throw new UnsupportedOperationException();
        }
    }

    private static class Worker extends Thread {
        private final ClusteredExecutor owner;
        private final int domain;

        private Worker(ThreadGroup group, Runnable task, String name, ClusteredExecutor owner, int domain) {
            super(group, task, name);
            setDaemon(true);
            this.owner = owner;
            this.domain = domain;
        }
    }

}
