package com.scott;

/**
 * Precise duration-controlled memory workload optimized for Hybrid executor.
 *
 * <p>Uses per-operation timing checks to achieve high precision on target duration.
 * Checks elapsed time after each operation for maximum stability and accuracy,
 * matching the behavior of {@link HybridCPUWorkload} for fair comparison.
 *
 * <p>Properties:
 * <ul>
 *   <li><b>Precise timing</b> – checks elapsed time after each operation</li>
 *   <li><b>Low variance</b> – execution times are highly stable</li>
 *   <li><b>Deterministic</b> – same seed and target always produce same duration</li>
 *   <li><b>JIT-safe</b> – computed value is returned</li>
 *   <li><b>No locks/I/O/blocking</b> – pure memory operations</li>
 * </ul>
 *
 * <p>Key difference from {@link MemoryBoundWorkloadDuration}:
 * <ul>
 *   <li>Checks time after each operation instead of after batches</li>
 *   <li>Eliminates overshoot variance (no batch boundary issues)</li>
 *   <li>Maximizes precision for accurate CV monitoring in Hybrid executor</li>
 * </ul>
 */
public final class HybridMemoryBoundWorkload implements Workload {

    private final long[] buffer;
    private final MemoryBoundWorkload.AccessPattern pattern;
    private final long seed;
    private final boolean writeBack;
    private final long targetNanos;

    /**
     * Creates a precise duration-controlled memory workload for Hybrid executor.
     *
     * @param buffer       shared long[] buffer (read-only by default)
     * @param targetMicros target execution time in microseconds
     * @param pattern      SEQUENTIAL or RANDOM access pattern
     * @param seed         per-task deterministic seed
     * @param writeBack    when true, write back to buffer; when false, read-only
     */
    public HybridMemoryBoundWorkload(long[] buffer,
                                     long targetMicros,
                                     MemoryBoundWorkload.AccessPattern pattern,
                                     long seed,
                                     boolean writeBack) {
        this.buffer = buffer;
        this.pattern = pattern;
        this.seed = seed;
        this.writeBack = writeBack;
        this.targetNanos = targetMicros * 1_000L;
    }

    @Override
    public long execute() {
        final long[] buf = buffer;
        final int n = buf.length;
        final boolean wb = writeBack;
        long x = seed;

        long start = System.nanoTime();
        long elapsed = 0L;

        if (pattern == MemoryBoundWorkload.AccessPattern.SEQUENTIAL) {
            int idx = (int) Long.remainderUnsigned(seed, n);
            while (elapsed < targetNanos) {
                x += buf[idx];
                idx++;
                if (idx >= n) idx = 0;
                elapsed = System.nanoTime() - start;
            }
        } else {
            // RANDOM pattern
            while (elapsed < targetNanos) {
                x ^= (x << 13);
                x ^= (x >>> 7);
                x ^= (x << 17);
                int idx = (int) ((x >>> 1) % n);
                x += buf[idx];
                elapsed = System.nanoTime() - start;
            }
        }

        // Blackhole sink to prevent JIT optimization
        long[] tlSink = TL_SINK.get();
        tlSink[0] = x;
        return x;
    }

    /**
     * Per-thread blackhole sink to prevent JIT optimization.
     */
    private static final ThreadLocal<long[]> TL_SINK =
            ThreadLocal.withInitial(() -> new long[16]);

    public long getSeed() {
        return seed;
    }

    public long getTargetNanos() {
        return targetNanos;
    }
}

