package com.scott;

/**
 * Lifecycle state of a single {@code DynamicHybridDispatcher} shard/worker.
 *
 * <p>Used only by the Dynamic Hybrid executor mode. Existing SHARED /
 * SHARDED / HYBRID modes do not reference this enum.
 *
 * <pre>
 *   scale-out:  INACTIVE  -> ACTIVATING -> ACTIVE
 *   scale-in:   ACTIVE    -> DRAINING   -> INACTIVE
 * </pre>
 *
 * <ul>
 *   <li>{@link #INACTIVE}  — worker is servicing the Shared/global queue;
 *       its local Sharded queue does not receive new SHORT tasks.</li>
 *   <li>{@link #ACTIVATING} — controller has requested the worker to
 *       switch to its local Sharded queue. The worker may still be
 *       running a Shared task; new SHORT tasks are NOT routed here yet.</li>
 *   <li>{@link #ACTIVE} — worker services its local Sharded queue and
 *       participates in the round-robin over active shards.</li>
 *   <li>{@link #DRAINING} — no new SHORT tasks are routed here. The
 *       worker drains what is already queued, then returns to INACTIVE.</li>
 * </ul>
 */
public enum ShardState {
    INACTIVE,
    ACTIVATING,
    ACTIVE,
    DRAINING
}

