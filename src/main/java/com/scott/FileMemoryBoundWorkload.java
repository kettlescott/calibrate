package com.scott;

/**
 * MEMORY workload that reads duration from a shared DurationFileTaskSource.
 * Each execute() call:
 *   1. Reads next duration from source
 *   2. Delegates to MemoryBoundWorkloadDuration(buffer, durationMicros, pattern, seed, writeBack, batchSize)
 *   3. Returns computed value (never 0, avoids JIT dead-code elimination)
 *
 * If file EOF is reached (nextDurationMicros() returns -1), throws IllegalStateException.
 */
public final class FileMemoryBoundWorkload implements Workload {
    private final DurationFileTaskSource source;
    private final long[] buffer;
    private final MemoryBoundWorkload.AccessPattern pattern;
    private final long seed;
    private final boolean writeBack;
    private final int batchSize;

    public FileMemoryBoundWorkload(DurationFileTaskSource source,
                                    long[] buffer,
                                    MemoryBoundWorkload.AccessPattern pattern,
                                    long seed,
                                    boolean writeBack,
                                    int batchSize) {
        this.source = source;
        this.buffer = buffer;
        this.pattern = pattern;
        this.seed = seed;
        this.writeBack = writeBack;
        this.batchSize = batchSize;
    }

    @Override
    public long execute() {
        long durationMicros = source.nextDurationMicros();
        if (durationMicros < 0) {
            throw new IllegalStateException(
                    "Duration file EOF reached: no more task durations available");
        }
        MemoryBoundWorkloadDuration mem = new MemoryBoundWorkloadDuration(
                buffer, durationMicros, pattern, seed, writeBack, batchSize);
        return mem.execute();
    }
}

