package com.netflix.sandbox;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class LinuxSchedulingBenchmark {

    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Benchmark
    public int currentCpu() {
        return LinuxScheduling.currentCpu();
    }

    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Benchmark
    public int nativeThreadId() {
        return LinuxScheduling.nativeThreadId();
    }
    
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(LinuxSchedulingBenchmark.class.getSimpleName())
            .warmupIterations(0)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

}
