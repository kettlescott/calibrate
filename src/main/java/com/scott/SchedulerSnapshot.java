package com.scott;

/**
 * Immutable snapshot of the scheduler state seen by a
 * {@link CapacityPolicy} at one controller tick.
 *
 * <p>All fields are already normalised so a policy can decide without
 * touching any dispatcher internals:
 * <ul>
 *   <li>{@code pressureNanos} — Ps = Qs·Ts/Ns (in nanoseconds). Set to
 *       {@link Long#MAX_VALUE} when Ns == 0 (treat as maximum pressure).</li>
 *   <li>{@code activeShardCount} — Ns, currently ACTIVE shards.</li>
 *   <li>{@code workerCount} — N, the fixed total number of worker
 *       threads. A policy must never request Ns &gt; N.</li>
 *   <li>{@code minShardedWorkers} — Nmin from {@link DynamicHybridConfig}.
 *       A policy must never request Ns &lt; Nmin.</li>
 *   <li>{@code maxShardedWorkers} - Nmax from {@link DynamicHybridConfig}.
 *       A policy must never request Ns &gt; Nmax.</li>
 *   <li>{@code sharedQueueHasWork} — whether the global Shared queue
 *       currently has at least one pending task. Scale-in is only
 *       meaningful when this is {@code true}.</li>
 *   <li>{@code scaleOutThresholdNanos} / {@code scaleInThresholdNanos}
 *       — H and L pre-converted to nanoseconds so the policy can
 *       compare directly against {@code pressureNanos} without touching
 *       the config.</li>
 * </ul>
 */
public record SchedulerSnapshot(
        long pressureNanos,
        int activeShardCount,
        int workerCount,
        int minShardedWorkers,
        int maxShardedWorkers,
        boolean sharedQueueHasWork,
        long scaleOutThresholdNanos,
        long scaleInThresholdNanos) {
}
