package com.scott;

/**
 * Configuration for HYBRID executor's adaptive CV-based switching.
 *
 * <p>Used only when a workload entry specifies {@code executor: HYBRID}.
 * Unused for SHARED or SHARDED executors.
 *
 * <h3>YAML shape</h3>
 * <pre>{@code
 * hybrid:
 *   initialMode: SHARDED       # Start in SHARDED or SHARED
 *   sharedEnterCv: 0.16        # CV threshold to switch to SHARED
 *   shardedEnterCv: 0.08       # CV threshold to switch to SHARDED
 *   windowMillis: 100          # Sliding window for CV calculation
 *   debugCvPrint: 0            # CV debug print interval in seconds; 0 disables
 *   targetMicros: 0            # (Optional) fixed target for targetDeviation; 0 = use standard CV
 * }</pre>
 *
 * <p>Invariants:
 * <ul>
 *   <li>{@code 0 <= shardedEnterCv < sharedEnterCv}</li>
 *   <li>{@code windowMillis > 0}</li>
 *   <li>{@code targetMicros >= 0} (0 means use standard CV)</li>
 * </ul>
 *
 * <p>CV thresholds define hysteresis: between them, the current mode
 * is maintained.
 *
 * <p>When targetMicros > 0, switching uses targetDeviation instead of CV:
 * targetDeviation = sqrt(Σ(x - T)² / N) / T, where T = targetMicros (in nanos).
 */
public record AdaptiveHybridConfig(
        ExecutorMode initialMode,
        double sharedEnterCv,
        double shardedEnterCv,
        long windowMillis,
        long debugCvPrintIntervalSeconds,
        long targetMicros
) {
    public AdaptiveHybridConfig(ExecutorMode initialMode,
                                double sharedEnterCv,
                                double shardedEnterCv,
                                long windowMillis) {
        this(initialMode, sharedEnterCv, shardedEnterCv, windowMillis, 0L, 0L);
    }

    public AdaptiveHybridConfig(ExecutorMode initialMode,
                                double sharedEnterCv,
                                double shardedEnterCv,
                                long windowMillis,
                                long debugCvPrintIntervalSeconds) {
        this(initialMode, sharedEnterCv, shardedEnterCv, windowMillis, debugCvPrintIntervalSeconds, 0L);
    }

    public AdaptiveHybridConfig(ExecutorMode initialMode,
        double sharedEnterCv,
        double shardedEnterCv,
        long windowMillis,
        long debugCvPrintIntervalSeconds, boolean only) {
        this(initialMode, sharedEnterCv, shardedEnterCv, windowMillis, debugCvPrintIntervalSeconds, 0L);
    }

    public AdaptiveHybridConfig {
        if (initialMode == null) {
            throw new IllegalArgumentException("hybrid.initialMode must not be null");
        }
        if (initialMode == ExecutorMode.HYBRID) {
            throw new IllegalArgumentException(
                    "hybrid.initialMode must be SHARED or SHARDED, not HYBRID");
        }
        if (sharedEnterCv < 0.0 || shardedEnterCv < 0.0) {
            throw new IllegalArgumentException(
                    "hybrid CV thresholds must be non-negative");
        }
        if (shardedEnterCv >= sharedEnterCv) {
//            throw new IllegalArgumentException(
//                    "hybrid.shardedEnterCv (" + shardedEnterCv + ") must be < sharedEnterCv ("
//                    + sharedEnterCv + ")");
        }
        if (windowMillis <= 0) {
            throw new IllegalArgumentException(
                    "hybrid.windowMillis must be > 0 (got " + windowMillis + ")");
        }
        if (debugCvPrintIntervalSeconds < 0L) {
            throw new IllegalArgumentException(
                    "hybrid.debugCvPrint must be >= 0 seconds (got " + debugCvPrintIntervalSeconds + ")");
        }
        if (targetMicros < 0) {
            throw new IllegalArgumentException(
                    "hybrid.targetMicros must be >= 0 (got " + targetMicros + ")");
        }
    }

    public String summary() {
        return "AdaptiveHybridConfig{initialMode=" + initialMode
                + ", sharedEnterCv=" + String.format("%.3f", sharedEnterCv)
                + ", shardedEnterCv=" + String.format("%.3f", shardedEnterCv)
                + ", windowMillis=" + windowMillis
                + ", debugCvPrint=" + debugCvPrintIntervalSeconds + "s"
                + ", targetMicros=" + targetMicros + "µs" + "}";
    }

    /**
     * Returns whether targetDeviation mode is enabled.
     */
    public boolean isTargetDeviationMode() {
        return targetMicros > 0;
    }

    /**
     * Returns target in nanoseconds (for internal calculations).
     */
    public long getTargetNanos() {
        return targetMicros * 1_000L;
    }

    public void validate() {
        // All validation done in constructor
    }
}

