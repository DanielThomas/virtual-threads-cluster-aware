package com.netflix.sandbox;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchedulingTest {

    @Test
    public void currentCpu() {
        int cpu = Scheduling.currentCpu();

        assertTrue(cpu >= 0);
    }

    @Test
    public void nativeThreadId() {
        int nid = Scheduling.nativeThreadId();

        assertTrue(nid > 0);
    }

    @Test
    public void currentThreadAffinity() {
        BitSet mask = Scheduling.currentThreadAffinity();

        assertEquals(Runtime.getRuntime().availableProcessors(), mask.cardinality());
    }

    @Test
    public void setsCurrentTheadAffinity() {
        BitSet mask = new BitSet();
        mask.set(0);
        Scheduling.currentThreadAffinity(mask);
        BitSet actual = Scheduling.currentThreadAffinity();

        assertEquals(mask, actual);
        assertEquals(0, Scheduling.currentCpu());
    }

    @Test
    public void setsHighestThreadAffinity() {
        int expected = Runtime.getRuntime().availableProcessors() - 1;
        BitSet mask = new BitSet();
        mask.set(expected);
        Scheduling.currentThreadAffinity(mask);
        BitSet actual = Scheduling.currentThreadAffinity();

        assertEquals(mask, actual);
        assertEquals(expected, Scheduling.currentCpu());
    }

    @Test
    public void invalidSetCurrentThreadAffinity() {
        BitSet invalidMask = new BitSet();
        invalidMask.set(1024);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> Scheduling.currentThreadAffinity(invalidMask));
        assertEquals("sched_setaffinity failed with errno: 22", e.getMessage());
    }

}