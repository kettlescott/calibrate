package com.scott;

/**
 * State machine for HYBRID executor mode transitions.
 *
 * <pre>
 * SHARDED ─────────────┐
 *    ↑                 ↓
 *    │           TO_SHARED (transition)
 *    │                 ↓
 *    └─────────── SHARED
 *
 * SHARED ──────────────┐
 *    ↑                 ↓
 *    │           TO_SHARDED (transition)
 *    │                 ↓
 *    └─────────── SHARDED
 * </pre>
 *
 * States:
 * - SHARDED: All workers consume from their own shard queue
 * - TO_SHARED: Transitioning from SHARDED to SHARED
 * - SHARED: All workers consume from the single shared queue
 * - TO_SHARDED: Transitioning from SHARED to SHARDED
 */
public enum ExecutorState {
    SHARDED("Sharded"),
    TO_SHARED("ToShared"),
    SHARED("Shared"),
    TO_SHARDED("ToSharded");

    private final String displayName;

    ExecutorState(String displayName) {
        this.displayName = displayName;
    }

    public String displayName() {
        return displayName;
    }

    /**
     * Is this a stable (non-transitional) state?
     */
    public boolean isStable() {
        return this == SHARDED || this == SHARED;
    }

    /**
     * Is this a transitional state?
     */
    public boolean isTransitional() {
        return this == TO_SHARED || this == TO_SHARDED;
    }
}

