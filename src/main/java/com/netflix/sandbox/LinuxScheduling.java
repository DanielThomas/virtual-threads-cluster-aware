package com.netflix.sandbox;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class LinuxScheduling {

    private LinuxScheduling() {
    }

    /**
     * Return the processors online for the current system.
     */
    public static IntStream onlineProcessors() {
        Path path = Path.of("/sys/devices/system/cpu/cpu/online");
        String onlineCpus = null;
        try {
            onlineCpus = Files.readString(path).trim();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return parseCpuList(onlineCpus);
    }

    /**
     * Return the processors available to the current thread.
     * <p>
     * The {@link IntStream#count()} of the result will equal {@link Runtime#availableProcessors()}.
     */
    public static IntStream availableProcessors() {
        return currentThreadAffinity().stream();
    }

    /**
     * Return the processor that the current thread is running on.
     */
    public static int currentProcessor() {
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

    /**
     * Return the processors that share a last-level cache (LLC) with the given processor.
     */
    public static IntStream llcSharedProcessors(int processorIndex) {
        Path path = Path.of("/sys/devices/system/cpu/cpu" + processorIndex, "cache");
        try (Stream<Path> dirs = Files.list(path).filter(Files::isDirectory)) {
            int highestIndex = dirs.map(Path::getFileName)
                .map(Path::toString)
                .filter(filename -> filename.startsWith("index"))
                .mapToInt(filename -> Integer.parseInt(filename.substring(5)))
                .max()
                .getAsInt();
            String sharedCpus = Files.readString(path.resolve("index" + highestIndex, "shared_cpu_list")).trim();
            return parseCpuList(sharedCpus);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static IntStream processorNodes(int processorIndex) {
        Path path = Path.of("/sys/devices/system/cpu/cpu" + processorIndex);
        return listNodes(path);
    }

    /**
     * Return the processor nodes in the current system.
     */
    public static IntStream processorNodes() {
        Path path = Path.of("/sys/devices/system/node");
        return listNodes(path);
    }

    public static int numCacheLevels(int processorIndex) {
        Path path = Path.of("/sys/devices/system/cpu/cpu" + processorIndex, "cache");
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

    public static int cacheSizeInBytes(int processorIndex, int cacheLevel) {
        Path path = Path.of("/sys/devices/system/cpu/cpu" + processorIndex, "cache", "index" + cacheLevel, "size");
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

    private static IntStream listNodes(Path path) {
        try (Stream<Path> dirs = Files.list(path).filter(Files::isDirectory)) {
            int[] nodes = dirs.map(Path::getFileName)
                .map(Path::toString)
                .filter(filename -> filename.startsWith("node"))
                .map(filename -> filename.substring(4))
                .mapToInt(Integer::parseInt)
                .toArray();
            return IntStream.of(nodes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static IntStream nodeProcessors(int nodeIndex) {
        Path path = Path.of("/sys/devices/system/node/node" + nodeIndex, "cpulist");
        try {
            String nodeProcessors = Files.readString(path).trim();
            return parseCpuList(nodeProcessors);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Return the native thread identifier for the current thread.
     */
    public static int nativeThreadId() {
        try {
            return (int) GETTID.invokeExact();
        } catch (Throwable e) {
            throw wrapChecked(e);
        }
    }

    /**
     * Return a {@link BitSet} describing the processor affinity for the current thread.
     */
    public static BitSet currentThreadAffinity() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
            MemorySegment cpu_set = arena.allocate(CPUSET_BYTE_SIZE);
            try {
                int result = (int) SCHED_GETAFFINITY.invokeExact(capturedState, nativeThreadId(), cpu_set.byteSize(), cpu_set);
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

    /**
     * Set the processor affinity for the current thread.
     */
    public static void currentThreadAffinity(BitSet cpus) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
            long[] longs = cpus.toLongArray();
            Objects.checkIndex(longs.length, CPUSET_SIZE);
            MemorySegment cpu_set = arena.allocateFrom(ValueLayout.JAVA_LONG, longs);
            try {
                int result = (int) SCHED_SETAFFINITY.invokeExact(capturedState, nativeThreadId(), cpu_set.byteSize(), cpu_set);
                if (result == -1) {
                    int errno = (int) CAPTURE_STATE.get(capturedState, 0L);
                    throw new RuntimeException("sched_setaffinity failed with errno: " + errno);
                }
            } catch (Throwable e) {
                throw wrapChecked(e);
            }
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

    private static RuntimeException wrapChecked(Throwable t) {
        switch (t) {
            case Error e -> throw e;
            case RuntimeException e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }

    // see https://github.com/bminor/glibc/blob/a2509a8bc955988f01f389a1cf74db3a9da42409/posix/bits/cpu-set.h#L27-L29
    private static final int CPUSET_SIZE = 1024 / (Byte.SIZE * (int) ValueLayout.JAVA_LONG.byteSize());
    private static final int CPUSET_BYTE_SIZE = 1024 / Byte.SIZE;

    private static final StructLayout CAPTURE_STATE_LAYOUT;
    private static final VarHandle CAPTURE_STATE;

    private static final MethodHandle SCHED_GETCPU;
    private static final MethodHandle GETTID;
    private static final MethodHandle SCHED_GETAFFINITY;
    private static final MethodHandle SCHED_SETAFFINITY;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup stdLib = linker.defaultLookup();

        Linker.Option ccs = Linker.Option.captureCallState("errno");
        CAPTURE_STATE_LAYOUT = Linker.Option.captureStateLayout();
        CAPTURE_STATE = CAPTURE_STATE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

        MemorySegment getcpu_addr = stdLib.findOrThrow("sched_getcpu");
        FunctionDescriptor getcpu_sig =
            FunctionDescriptor.of(ValueLayout.JAVA_INT);
        SCHED_GETCPU = linker.downcallHandle(getcpu_addr, getcpu_sig, ccs);

        MemorySegment gettid_addr = stdLib.findOrThrow("gettid");
        FunctionDescriptor gettid_sig =
            FunctionDescriptor.of(ValueLayout.JAVA_INT);
        GETTID = linker.downcallHandle(gettid_addr, gettid_sig);

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
