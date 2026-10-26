package com.scott;

/**
 * Duration-controlled CPU-bound workload.
 *
 * <p>Runs CPU operations for a target duration rather than a fixed iteration count.
 * Performs batches of iterations and checks elapsed time between batches to minimize
 * timing overhead (avoid System.nanoTime() call per iteration).
 *
 * <p>Preserves all behavioural properties of {@link CpuBoundWorkload}:
 * - Deterministic mixing function (XOR-shift + golden-ratio)
 * - JIT-safe (computed value is returned)
 * - No locks / blocking / I/O
 * - Configurable intensity via batch size and deadline
 *
 * <h3>Batch timing</h3>
 * The workload performs {@code batchSize} iterations, then checks if the target
 * duration has been reached. This amortises the cost of {@link System#nanoTime()}.
 * The actual execution time will exceed the target by at most one batch duration.
 */
public final class CpuBoundWorkloadDuration implements Workload {

    private final long targetNanos;
    private final long seed;
    private final int batchSize;

    /**
     * Creates a duration-controlled CPU workload.
     *
     * @param targetMicros target execution time in microseconds
     * @param seed         per-task deterministic seed
     * @param batchSize    number of iterations per batch before checking elapsed time
     */
    public CpuBoundWorkloadDuration(long targetMicros, long seed, int batchSize) {
        this.targetNanos = targetMicros * 1_000L;
        this.seed = seed;
        this.batchSize = Math.max(1, batchSize);
    }

    @Override
    public long execute() {
        long x = seed;
        long start = System.nanoTime();
        long elapsed = 0L;
        final int bs = batchSize;
        long counter = 0L;

        while (elapsed < targetNanos) {
            // Perform one batch of iterations
            for (int i = 0; i < bs; i++) {
                x ^= (x << 13);
                x ^= (x >>> 7);
                x ^= (x << 17);
                x = x * 0x9E3779B97F4A7C15L + counter;
                counter++;
            }
            // Check deadline only after batch (amortizes nanoTime cost)
            elapsed = System.nanoTime() - start;
        }

        return x;
    }

    public long getSeed() {
        return seed;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getTargetNanos() {
        return targetNanos;
    }
}

