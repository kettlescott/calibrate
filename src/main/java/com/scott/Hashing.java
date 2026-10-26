package com.scott;

/**
 * Centralised routing-hash helpers shared by {@link ShardedExecutor}
 * (submit-time routing) and {@link ShardLatencyAnalyzer} (post-hoc
 * shard derivation). Keeping a single source of truth ensures submit
 * and analysis cannot drift.
 *
 * <p>{@link #mix64(long)} is the SplitMix64 finalizer — a strong
 * avalanching 64-bit mix. It is allocation-free and JIT-inlinable.
 * The routing path is on the hot submit path; benchmark a JIT spot
 * check with {@code -XX:+PrintInlining} if questioned.</p>
 */
public final class Hashing {

    private Hashing() {}

    /** SplitMix64 finalizer — strong 64-bit avalanche mix. */
    public static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    /**
     * Computes the destination shard for {@code taskId} under
     * {@code workerCount} workers using the configured routing mode.
     *
     * <ul>
     *   <li>{@code MODULO} — legacy behaviour:
     *       {@code floorMod(Long.hashCode(taskId), workerCount)}.
     *       With sequential taskIds and a power-of-two workerCount
     *       this is effectively {@code taskId % workerCount}, which
     *       couples taskId residue classes (and therefore workload
     *       seed residue classes) to specific shards.</li>
     *   <li>{@code MIXED_HASH} — applies the SplitMix64 finalizer to
     *       {@code taskId ^ routingSeed} before mapping to a shard:
     *       {@code floorMod(Long.hashCode(mix64(taskId ^ routingSeed)),
     *       workerCount)}. This breaks the taskId↔shard residue-class
     *       coupling that can manifest as persistent per-shard
     *       latency artifacts on memory-bound runs.</li>
     * </ul>
     */
    public static int shardOf(long taskId, int workerCount, ShardedRoutingConfig cfg) {
        if (cfg == null || cfg.mode() == ShardedRoutingConfig.Mode.MODULO) {
            return Math.floorMod(Long.hashCode(taskId), workerCount);
        }
        long mixed = mix64(taskId ^ cfg.routingSeed());
        return Math.floorMod(Long.hashCode(mixed), workerCount);
    }
}

