package com.scott;

import com.scott.DynamicHybridDispatcher.Decision;

/**
 * Default {@link CapacityPolicy} — the EWMA + pressure + hysteresis rule
 * from the Dynamic Hybrid spec:
 *
 * <pre>
 *   if Ps &gt; H                                     -> SCALE_OUT (if Ns &lt; N)
 *   else if Ps &lt; L
 *         AND shared queue has pending work
 *         AND Ns &gt; Nmin                            -> SCALE_IN
 *   else                                             -> NONE
 * </pre>
 *
 * <p>Stateless singleton — safe to share across dispatcher instances.
 */
public final class HysteresisPressurePolicy implements CapacityPolicy {

    public static final HysteresisPressurePolicy INSTANCE = new HysteresisPressurePolicy();

    private HysteresisPressurePolicy() {}

    @Override
    public Decision evaluate(SchedulerSnapshot s) {
        if (s.pressureNanos() > s.scaleOutThresholdNanos()) {
            // Never let Ns exceed maxShardedWorkers, so at least
            // minSharedWorkers workers always service LONG tasks.
            return (s.activeShardCount() < s.maxShardedWorkers())
                    ? Decision.SCALE_OUT : Decision.NONE;
        }
        if (s.pressureNanos() < s.scaleInThresholdNanos()
                && s.activeShardCount() > s.minShardedWorkers()
                && s.sharedQueueHasWork()) {
            return Decision.SCALE_IN;
        }
        return Decision.NONE;
    }
}

