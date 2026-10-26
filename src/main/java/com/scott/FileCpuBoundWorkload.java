package com.scott;

/**
 * CPU workload that reads duration from a shared DurationFileTaskSource.
 * Each execute() call:
 *   1. Reads next duration from source
 *   2. Delegates to CpuBoundWorkloadDuration(durationMicros, seed, batchSize)
 *   3. Returns computed value (never 0, avoids JIT dead-code elimination)
 *
 * If file EOF is reached (nextDurationMicros() returns -1), throws IllegalStateException.
 */
public final class FileCpuBoundWorkload implements Workload {
    private final DurationFileTaskSource source;
    private final long seed;
    private final int batchSize;

    public FileCpuBoundWorkload(DurationFileTaskSource source, long seed, int batchSize) {
        this.source = source;
        this.seed = seed;
        this.batchSize = batchSize;
    }

    @Override
    public long execute() {
        long durationMicros = source.nextDurationMicros();
        if (durationMicros < 0) {
            throw new IllegalStateException(
                    "Duration file EOF reached: no more task durations available");
        }
        CpuBoundWorkloadDuration cpu = new CpuBoundWorkloadDuration(durationMicros, seed, batchSize);
        return cpu.execute();
    }
}

