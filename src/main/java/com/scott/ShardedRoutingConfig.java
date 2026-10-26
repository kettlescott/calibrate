package com.scott;

/**
 * Configuration for the sharded routing function.
 *
 * <p>Knob added to rule out a benchmark artifact in memory-bound
 * sharded runs: with the legacy {@code MODULO} routing, taskId
 * residue classes map deterministically to shards, which couples each
 * shard to a fixed slice of the workload's per-task RNG seed space
 * ({@code seed + taskId}). If some seed residue classes generate
 * slower random memory-access traces, the benchmark falsely reports a
 * persistent per-shard latency gap. {@code MIXED_HASH} breaks that
 * coupling.</p>
 */
public record ShardedRoutingConfig(Mode mode, long routingSeed) {

    public enum Mode { MODULO, MIXED_HASH }

    public static ShardedRoutingConfig defaults() {
        return new ShardedRoutingConfig(Mode.MODULO, 0L);
    }

    public static Mode parseMode(String raw) {
        if (raw == null) return Mode.MODULO;
        String r = raw.trim().toUpperCase();
        return switch (r) {
            case "MODULO"      -> Mode.MODULO;
            case "MIXED_HASH"  -> Mode.MIXED_HASH;
            default -> throw new IllegalArgumentException(
                    "Unknown shardedRouting.mode: " + raw + " (expected MODULO|MIXED_HASH)");
        };
    }

    public String describe() {
        return mode == Mode.MODULO
                ? "MODULO"
                : "MIXED_HASH(routingSeed=" + routingSeed + ")";
    }
}

