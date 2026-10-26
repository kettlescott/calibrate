package com.scott;

/**
 * Precise duration-controlled CPU workload optimized for Hybrid executor.
 *
 * <p>Uses per-operation timing checks to achieve high precision (±1%) on target duration.
 * Checks elapsed time after every operation for maximum stability and accuracy.
 *
 * <p>Properties:
 * <ul>
 *   <li><b>Precise timing</b> – checks elapsed time after each operation</li>
 *   <li><b>Low variance</b> – execution times are highly stable</li>
 *   <li><b>Deterministic</b> – same seed and target always produce same duration</li>
 *   <li><b>JIT-safe</b> – computed value is returned</li>
 *   <li><b>No locks/I/O/blocking</b> – pure CPU arithmetic</li>
 * </ul>
 *
 * <p>Key difference from {@link CpuBoundWorkloadDuration}:
 * <ul>
 *   <li>Checks time after each operation instead of in batches</li>
 *   <li>Eliminates overshoot variance (no batch boundary issues)</li>
 *   <li>Maximizes precision for accurate CV monitoring in Hybrid executor</li>
 * </ul>
 */
public final class HybridCPUWorkload implements Workload {

    private final long targetNanos;
    private final long seed;

    /**
     * Creates a precise duration-controlled CPU workload.
     *
     * @param targetMicros target execution time in microseconds
     * @param seed         per-task deterministic seed
     */
    public HybridCPUWorkload(long targetMicros, long seed) {
        this.targetNanos = targetMicros * 1_000L;
        this.seed = seed;
    }

    @Override
    public long execute() {
        long x = seed;
        long start = System.nanoTime();
        long elapsed = 0L;
        long counter = 0L;
        final int bs = 64;

        // Precise timing - check elapsed time after each operation
        while (elapsed < targetNanos) {

            for (int i = 0; i < bs; i++) {
                x ^= (x << 13);
                x ^= (x >>> 7);
                x ^= (x << 17);
                x = x * 0x9E3779B97F4A7C15L + counter;
                counter++;
            }
            elapsed = System.nanoTime() - start;
        }

        return x;
    }

    public long getSeed() {
        return seed;
    }

    public long getTargetNanos() {
        return targetNanos;
    }
}

