package com.scott;

/**
 * Duration-controlled memory-bound workload.
 *
 * <p>Runs memory operations for a target duration with microsecond-level precision.
 * Checks elapsed time periodically (every 4 operations) to balance timing accuracy
 * and nanoTime() call overhead.
 *
 * <p>Preserves all the behavioural properties of {@link MemoryBoundWorkload}:
 * - SEQUENTIAL or RANDOM access patterns
 * - Shared read-only buffer (or read-modify-write with writeBack=true)
 * - Deterministic pseudo-random seeding
 * - JIT-defeating blackhole sink
 *
 * <h3>Timing precision</h3>
 * Time is checked every 4 operations via {@link System#nanoTime()}.
 * This ensures accurate duration control while minimizing nanoTime() overhead.
 * Result: microsecond-level precision with good performance.
 */
public final class MemoryBoundWorkloadDuration implements Workload {

    private final long[] buffer;
    private final MemoryBoundWorkload.AccessPattern pattern;
    private final long seed;
    private final boolean writeBack;
    private final long targetNanos;

    /**
     * Creates a duration-controlled memory workload.
     *
     * @param buffer        shared long[] buffer (read-only by default)
     * @param targetMicros  target execution time in microseconds
     * @param pattern       SEQUENTIAL or RANDOM access pattern
     * @param seed          per-task deterministic seed
     * @param writeBack     when true, write back to buffer; when false, read-only
     * @param batchSize     (deprecated: no longer used, time check on each operation)
     */
    public MemoryBoundWorkloadDuration(long[] buffer,
                                       long targetMicros,
                                       MemoryBoundWorkload.AccessPattern pattern,
                                       long seed,
                                       boolean writeBack,
                                       @SuppressWarnings("unused") int batchSize) {
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
        long totalOps = 0L;

        long start = System.nanoTime();
        long elapsed = 0L;
        
        // Check time every 4 operations to balance precision and nanoTime() overhead
        final int CHECK_INTERVAL = 4;

        if (pattern == MemoryBoundWorkload.AccessPattern.SEQUENTIAL) {
            int idx = (int) Long.remainderUnsigned(seed, n);
            while (elapsed < targetNanos) {
                // Perform CHECK_INTERVAL operations
                for (int i = 0; i < CHECK_INTERVAL; i++) {
                    x += buf[idx];
                    if (wb) buf[idx] = x;
                    idx++;
                    if (idx >= n) idx = 0;
                    totalOps++;
                }
                elapsed = System.nanoTime() - start;
            }
        } else {
            // RANDOM pattern
            while (elapsed < targetNanos) {
                // Perform CHECK_INTERVAL operations
                for (int i = 0; i < CHECK_INTERVAL; i++) {
                    x ^= (x << 13);
                    x ^= (x >>> 7);
                    x ^= (x << 17);
                    int idx = (int) ((x >>> 1) % n);
                    x += buf[idx];
                    if (wb) buf[idx] = x;
                    totalOps++;
                }
                elapsed = System.nanoTime() - start;
            }
        }

        // Store memory operations count in a thread-local for diagnostics
        // (if needed by enhanced TaskGenerator)
        memoryOperationsCounts.set(totalOps);

        // Blackhole sink (same as MemoryBoundWorkload)
        // Use the same blackhole strategy as MemoryBoundWorkload for consistency
        long[] tlSink = TL_SINK.get();
        tlSink[0] = x;  // Always use thread-local to avoid cross-core invalidation
        return x;
    }

    // Diagnostic support: track memory operations per task
    private static final ThreadLocal<Long> memoryOperationsCounts =
            ThreadLocal.withInitial(() -> 0L);

    /**
     * Per-thread blackhole sink to prevent JIT optimization.
     */
    private static final ThreadLocal<long[]> TL_SINK =
            ThreadLocal.withInitial(() -> new long[16]);
}

