package com.scott;

/**
 * Selects which executor(s) to benchmark in a single JVM invocation.
 *
 * <p>Running each mode in a <em>separate</em> process produces the cleanest
 * JFR recordings — no cross-contamination of JIT compilation history,
 * GC generation state, or thread pools between executor designs.
 *
 * <h3>Usage</h3>
 * <pre>
 *   java ... BenchmarkMain --mode=prepare   # calibrate once, print fixed config
 *   java ... BenchmarkMain --mode=shared    # SharedExecutor only
 *   java ... BenchmarkMain --mode=sharded   # ShardedExecutor only
 *   java ... BenchmarkMain --mode=compare   # both, side-by-side (legacy)
 * </pre>
 */
public enum BenchmarkMode {

    /** Calibrate-only mode (legacy CLI tooling). */
    PREPARE,

    /** Run only {@link SharedExecutor}. */
    SHARED,

    /** Run only {@link ShardedExecutor}. */
    SHARDED,

    /** Run {@link HybridDispatcher} routing by {@link WorkloadKind}. */
    HYBRID,

    /** Run {@link DynamicHybridDispatcher} — adaptive Sharded/Shared capacity. */
    DYNAMIC_HYBRID,

    /** Run all policies sequentially (legacy comparison). */
    COMPARE;

    /**
     * Parses the mode from a {@code --mode=<value>} command-line argument.
     *
     * <p>Accepts the first argument that starts with {@code --mode=} (case-
     * insensitive value).  If no matching argument is found, returns
     * {@link #COMPARE} as the default to preserve backward compatibility.
     *
     * @param args command-line arguments
     * @return the parsed mode, or {@code COMPARE} if none specified
     */
    public static BenchmarkMode fromArgs(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--mode=")) {
                String value = arg.substring("--mode=".length()).trim().toUpperCase();
                try {
                    return BenchmarkMode.valueOf(value);
                } catch (IllegalArgumentException e) {
                    System.err.printf("Unknown benchmark mode: '%s'.  Valid modes: prepare, shared, sharded, hybrid, compare%n", value);
                    System.exit(1);
                }
            }
        }
        // Default: compare mode (preserves old behaviour)
        return COMPARE;
    }

    public static BenchmarkMode fromConfigValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Run mode is required");
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        // Legacy alias.
        if ("TYPE_AWARE".equals(normalized)) normalized = "HYBRID";
        try {
            return BenchmarkMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown run mode: '" + value + "' (expected shared|sharded|hybrid|dynamic_hybrid|compare|prepare)");
        }
    }
}
