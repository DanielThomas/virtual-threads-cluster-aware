package com.netflix.sandbox;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LinuxSchedulingTest {

    @Test
    public void onlineCpusMatchesAvailable() {
        long count = LinuxScheduling.onlineCpus().count();
        assertEquals(Runtime.getRuntime().availableProcessors(), count);
    }

    @Test
    public void sharedCpusArePresent() {
        IntStream sharedCpus = LinuxScheduling.sharedCpus(0);
        assertTrue(sharedCpus.findAny().isPresent());
    }
    
    @Test
    public void currentCpu() {
        int cpu = LinuxScheduling.currentCpu();

        assertTrue(cpu >= 0);
    }

    @Test
    public void nativeThreadId() {
        int nid = LinuxScheduling.nativeThreadId();

        assertTrue(nid > 0);
    }

    @Test
    public void currentThreadAffinity() {
        BitSet mask = LinuxScheduling.currentThreadAffinity();

        assertEquals(Runtime.getRuntime().availableProcessors(), mask.cardinality());
    }

    @Test
    public void setAndSetCurrentTheadAffinity() {
        BitSet current = LinuxScheduling.currentThreadAffinity();
        IntStream.range(0, Runtime.getRuntime().availableProcessors()).forEach(cpu -> {
            BitSet cpus = new BitSet();
            cpus.set(cpu);
            LinuxScheduling.currentThreadAffinity(cpus);
            BitSet actual = LinuxScheduling.currentThreadAffinity();
    
            assertEquals(cpus, actual);
            assertEquals(cpu, LinuxScheduling.currentCpu());
            assertEquals(1, Runtime.getRuntime().availableProcessors());
        });
        LinuxScheduling.currentThreadAffinity(current);
    }

    @Test
    public void invalidSetCurrentThreadAffinity() {
        BitSet invalidMask = new BitSet();
        invalidMask.set(Runtime.getRuntime().availableProcessors());
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> LinuxScheduling.currentThreadAffinity(invalidMask));
        assertEquals("sched_setaffinity failed with errno: 22", e.getMessage());
    }

    @Test
    public void indexOutOfBoundsForCpusGreaterThan1024() {
        BitSet invalidMask = new BitSet();
        invalidMask.set(1025);
        invalidMask.set(Runtime.getRuntime().availableProcessors());
        IndexOutOfBoundsException e = Assertions.assertThrows(IndexOutOfBoundsException.class, () -> LinuxScheduling.currentThreadAffinity(invalidMask));
        assertEquals("Index 17 out of bounds for length 16", e.getMessage());
    }
    
}
