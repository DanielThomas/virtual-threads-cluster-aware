package com.netflix.sandbox;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.BitSet;

public final class Scheduling {

    private Scheduling() {
    }

    public static int currentCpu() {
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

    public static int nativeThreadId() {
        try {
            return (int) GETTID.invokeExact();
        } catch (Throwable e) {
            throw wrapChecked(e);
        }        
    }

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

    public static void currentThreadAffinity(BitSet mask) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment capturedState = arena.allocate(CAPTURE_STATE_LAYOUT);
            long[] longs = Arrays.copyOf(mask.toLongArray(), CPUSET_SIZE);
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
    
    private static RuntimeException wrapChecked(Throwable t) {
        switch(t) {
            case Error e -> throw e;
            case RuntimeException e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }

    // see https://github.com/bminor/glibc/blob/a2509a8bc955988f01f389a1cf74db3a9da42409/posix/bits/cpu-set.h#L27-L29
    private static final int CPUSET_SIZE = 1024 / (Byte.SIZE * (int)ValueLayout.JAVA_LONG.byteSize());
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