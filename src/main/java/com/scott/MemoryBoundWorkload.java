package com.scott;

/**
 * A synthetic, deterministic memory-bound workload.
 *
 * <p>Walks a shared {@code long[]} buffer either sequentially or in a
 * deterministic pseudo-random order so that cache / TLB behaviour dominates
 * the runtime. Per-task determinism comes from {@code seed}; the buffer
 * itself is allocated once (per component) by {@link TaskGenerator} and
 * shared across tasks to keep per-task allocation cost near zero.
 *
 * <h3>Read-only by default</h3>
 * <p>By default the workload only <em>reads</em> the shared buffer.
 * Writing back to a shared buffer from many concurrent worker threads
 * causes false sharing and write-back traffic that confounds queue /
 * dispatcher measurements (and is a different experimental variable
 * altogether). Pass {@code writeBack=true} to opt-in to the
 * shared-write variant for a future "memory contention" experiment.
 *
 * <p>The computed value is returned so the JIT cannot eliminate the loop.
 */
public final class MemoryBoundWorkload implements Workload {

    public enum AccessPattern { SEQUENTIAL, RANDOM }

    private final long[] buffer;
    private final int steps;
    private final AccessPattern pattern;
    private final long seed;
    private final boolean writeBack;

    /** Read-only by default. */
    public MemoryBoundWorkload(long[] buffer, int steps, AccessPattern pattern, long seed) {
        this(buffer, steps, pattern, seed, false);
    }

    public MemoryBoundWorkload(long[] buffer, int steps, AccessPattern pattern, long seed, boolean writeBack) {
        this.buffer = buffer;
        this.steps = steps;
        this.pattern = pattern;
        this.seed = seed;
        this.writeBack = writeBack;
    }

    @Override
    public long execute() {
        final long[] buf = buffer;
        final int n = buf.length;
        final boolean wb = writeBack;
        long x = seed;
        if (pattern == AccessPattern.SEQUENTIAL) {
            int idx = (int) Long.remainderUnsigned(seed, n);
            for (int i = 0; i < steps; i++) {
                x += buf[idx];
                if (wb) buf[idx] = x;
                idx++;
                if (idx >= n) idx = 0;
            }
        } else {
            for (int i = 0; i < steps; i++) {
                x ^= (x << 13);
                x ^= (x >>> 7);
                x ^= (x << 17);
                int idx = (int) ((x >>> 1) % n);
                // Data-dependent READ of buf[idx] folded back into x.
                // Without this, the writeBack=false RANDOM branch never
                // touches buf and the JIT collapses the loop to pure
                // xorshift ALU — making execution time orders of
                // magnitude smaller than the intended memory-bound cost
                // (observed: p50 ≈ 1 µs for steps=320 on 512 MiB buffer,
                // which is physically impossible for real RANDOM access).
                // Mirror of the SEQUENTIAL branch's `x += buf[idx]`.
                x += buf[idx];
                if (wb) buf[idx] = x;
            }
        }
        // Defeat aggressive dead-store elimination of the returned value
        // by writing into a sink. Task already stores workload.execute()
        // into a field, but that field can in principle be proven dead
        // by escape analysis; the sink store is a cheap insurance policy
        // outside the inner loop.
        //
        // Sink strategy is process-wide and configurable
        // (see {@link BlackholeMode}). The default SHARED_VOLATILE
        // preserves legacy behaviour. THREAD_LOCAL eliminates the
        // cross-core cache-line invalidation that the shared volatile
        // store causes on memory-bound runs.
        if (BLACKHOLE_MODE == BlackholeMode.THREAD_LOCAL) {
            TL_SINK.get()[0] = x;
        } else {
            BLACKHOLE = x;
        }
        return x;
    }

    /** Volatile sink — prevents JIT from proving execute()'s result is dead. */
    @SuppressWarnings("unused")
    private static volatile long BLACKHOLE;

    /**
     * Per-thread sink. Length-16 array padded to a cache line so adjacent
     * threads cannot accidentally share the same line via heap proximity.
     * {@code ThreadLocal.get()} keeps the JIT from proving the store dead.
     */
    private static final ThreadLocal<long[]> TL_SINK =
            ThreadLocal.withInitial(() -> new long[16]);

    /**
     * Active blackhole strategy. Set once at startup via
     * {@link #configureBlackhole(BlackholeMode)}; read on the hot path
     * exactly once per task. A {@code volatile} read is cheap on x86
     * and amortised over the per-task workload loop.
     */
    private static volatile BlackholeMode BLACKHOLE_MODE = BlackholeMode.SHARED_VOLATILE;

    /**
     * Selects the blackhole sink strategy. Must be called before workers
     * begin executing tasks; safe to call again only when no measurement
     * is in flight (e.g., between runs).
     */
    public static void configureBlackhole(BlackholeMode mode) {
        BLACKHOLE_MODE = (mode == null) ? BlackholeMode.SHARED_VOLATILE : mode;
    }

    /** Diagnostic accessor — used by BenchmarkMain to emit summary lines. */
    public static BlackholeMode currentBlackholeMode() {
        return BLACKHOLE_MODE;
    }

    public static AccessPattern parsePattern(String raw) {
        if (raw == null) return AccessPattern.SEQUENTIAL;
        if (raw.equalsIgnoreCase("sequential")) return AccessPattern.SEQUENTIAL;
        if (raw.equalsIgnoreCase("random"))     return AccessPattern.RANDOM;
        throw new IllegalArgumentException("Unknown accessPattern: " + raw);
    }
}
