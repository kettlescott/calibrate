package com.scott;

/**
 * YAML-driven experimental parameters for {@link DynamicHybridDispatcher}.
 *
 * <p>Populated by {@link BenchmarkConfigLoader} only when the config file
 * contains a {@code dynamicHybrid:} section, and used only when a run
 * selects {@link BenchmarkMode#DYNAMIC_HYBRID}. Existing SHARED /
 * SHARDED / HYBRID benchmarks ignore it.
 *
 * <h3>YAML shape</h3>
 * <pre>{@code
 * dynamicHybrid:
 *   crossoverThresholdMicros: 200   # Tc
 *   minShardedWorkers:        4     # Nmin (floor on ACTIVE)
 *   minSharedWorkers:         1     # >=1, floor on shared servicers
 *   ewmaAlpha:                0.2   # alpha in (0,1]
 *   scaleOutThresholdMicros:  500   # H
 *   scaleInThresholdMicros:   150   # L (must be < H)
 *   controllerIntervalMicros: 1000  # controller tick period
 * }</pre>
 *
 * <p>Invariant: {@code minShardedWorkers + minSharedWorkers <= N}, so the
 * dispatcher can always honour both floors. {@code minSharedWorkers >= 1}
 * guarantees LONG tasks always have a servicer (no starvation).
 */
public record DynamicHybridConfig(
        long crossoverThresholdMicros,
        int minShardedWorkers,
        int minSharedWorkers,
        double ewmaAlpha,
        long scaleOutThresholdMicros,
        long scaleInThresholdMicros,
        long controllerIntervalMicros) {

    /** Fail-fast validation. Called once at startup by {@link RootConfig#validate()}. */
    public void validate(int totalWorkers) {
        if (crossoverThresholdMicros < 0) {
            throw new IllegalArgumentException("dynamicHybrid.crossoverThresholdMicros must be >= 0 (got " + crossoverThresholdMicros + ")");
        }
        if (minShardedWorkers < 0) {
            throw new IllegalArgumentException("dynamicHybrid.minShardedWorkers must be >= 0 (got " + minShardedWorkers + ")");
        }
        if (minShardedWorkers > totalWorkers) {
            throw new IllegalArgumentException("dynamicHybrid.minShardedWorkers (" + minShardedWorkers + ") must be <= global.workerCount (" + totalWorkers + ")");
        }
        if (minSharedWorkers < 1) {
            throw new IllegalArgumentException(
                    "dynamicHybrid.minSharedWorkers must be >= 1 to guarantee LONG-task progress (got "
                            + minSharedWorkers + ")");
        }
        if (minShardedWorkers + minSharedWorkers > totalWorkers) {
            throw new IllegalArgumentException(
                    "dynamicHybrid.minShardedWorkers (" + minShardedWorkers
                            + ") + dynamicHybrid.minSharedWorkers (" + minSharedWorkers
                            + ") must be <= global.workerCount (" + totalWorkers + ")");
        }
        if (!(ewmaAlpha > 0.0 && ewmaAlpha <= 1.0)) {
            throw new IllegalArgumentException("dynamicHybrid.ewmaAlpha must be in (0, 1] (got " + ewmaAlpha + ")");
        }
        if (scaleInThresholdMicros < 0) {
            throw new IllegalArgumentException("dynamicHybrid.scaleInThresholdMicros must be >= 0 (got " + scaleInThresholdMicros + ")");
        }
        if (scaleOutThresholdMicros <= scaleInThresholdMicros) {
            throw new IllegalArgumentException("dynamicHybrid.scaleOutThresholdMicros (" + scaleOutThresholdMicros + ") must be > scaleInThresholdMicros (" + scaleInThresholdMicros + ")");
        }
        if (controllerIntervalMicros <= 0) {
            throw new IllegalArgumentException("dynamicHybrid.controllerIntervalMicros must be > 0 (got " + controllerIntervalMicros + ")");
        }
    }

    /** Cap on ACTIVE shards: {@code N - minSharedWorkers}. */
    public int maxShardedWorkers(int totalWorkers) {
        return totalWorkers - minSharedWorkers;
    }

    public String policyDescription() {
        return "Tc=" + crossoverThresholdMicros + "us,Nmin=" + minShardedWorkers
                + ",MinShared=" + minSharedWorkers
                + ",alpha=" + ewmaAlpha + ",H=" + scaleOutThresholdMicros + "us"
                + ",L=" + scaleInThresholdMicros + "us,tick=" + controllerIntervalMicros + "us";
    }
}
