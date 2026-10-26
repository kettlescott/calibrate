package com.scott;

/**
 * A purely CPU-bound workload for executor latency benchmarking.
 *
 * <p>Properties:
 * <ul>
 *   <li><b>Deterministic</b> – identical {@code seed} and {@code iterations} always
 *       produce the same result.</li>
 *   <li><b>No sleep / blocking / I/O / locks / randomness</b> – exercises only
 *       arithmetic and bitwise operations in a tight loop.</li>
 *   <li><b>JIT-safe</b> – the computed value is returned (and should be consumed by
 *       the caller) so the loop cannot be eliminated as dead code.</li>
 *   <li><b>Configurable intensity</b> – the {@code iterations} parameter controls
 *       how much CPU time the workload consumes.</li>
 * </ul>
 *
 * <p>The mixing function combines XOR-shifts with a golden-ratio multiplicative
 * hash, producing a well-distributed bit pattern at every step.
 */
public final class CpuBoundWorkload implements Workload {

    private final long seed;
    private final int iterations;

    /**
     * @param seed       initial value fed into the mixing loop
     * @param iterations number of loop iterations (controls CPU intensity)
     */
    public CpuBoundWorkload(long seed, int iterations) {
        this.seed = seed;
        this.iterations = iterations;
    }

    /**
     * Runs a CPU-intensive deterministic loop and returns the final computed value.
     *
     * @return the mixed {@code long} result – must be consumed by the caller
     *         to prevent JIT dead-code elimination
     */
    @Override
    public long execute() {
        return execute(seed, iterations);
    }

    /**
     * Allocation-free helper for hot paths that already hold seed/iterations.
     */
    public static long execute(long seed, int iterations) {
        long x = seed;
        for (int i = 0; i < iterations; i++) {
            x ^= (x << 13);
            x ^= (x >>> 7);
            x ^= (x << 17);
            x = x * 0x9E3779B97F4A7C15L + i;
        }
        return x;
    }

    /* ---- accessors (useful for logging / diagnostics) ---- */

    public long getSeed() {
        return seed;
    }

    public int getIterations() {
        return iterations;
    }
}

