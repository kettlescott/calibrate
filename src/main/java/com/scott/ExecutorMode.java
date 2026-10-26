package com.scott;

/**
 * Executor selection for task routing.
 *
 * <ul>
 *   <li>{@code SHARED}  — All tasks to a single shared queue.</li>
 *   <li>{@code SHARDED} — Each task routed to a per-worker queue.</li>
 *   <li>{@code HYBRID}  — Adaptive: starts at initialMode, switches based on CV.</li>
 * </ul>
 */
public enum ExecutorMode {
    SHARED,
    SHARDED,
    HYBRID;

    public static ExecutorMode parse(String raw) {
        if (raw == null) {
            return SHARDED; // default
        }
        String v = raw.trim().toUpperCase();
        return switch (v) {
            case "SHARED"  -> SHARED;
            case "SHARDED" -> SHARDED;
            case "HYBRID"  -> HYBRID;
            default -> throw new IllegalArgumentException(
                    "Unknown ExecutorMode: '" + raw + "' (allowed: SHARED|SHARDED|HYBRID)");
        };
    }
}

