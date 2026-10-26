package com.scott;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Fully YAML-driven configuration for {@link HybridDispatcher}.
 *
 * <p>There are <em>no</em> built-in routing defaults. Every workload
 * kind ({@link WorkloadKind#CPU}, {@link WorkloadKind#MEMORY}) must be explicitly mapped to a
 * {@link RouteTarget} ({@code SHARED} or {@code SHARDED}).
 *
 * <p>This makes routing policy a first-class experimental variable —
 * the same binary can run policy A (CPU→SHARDED, MEMORY→SHARED),
 * policy B (CPU→SHARED, MEMORY→SHARDED), etc.
 * without recompilation.
 *
 * <h3>YAML shape</h3>
 * <pre>{@code
 * hybrid:
 *   sharedWorkers: 8
 *   shardedWorkers: 8
 *   routing:
 *     CPU:    SHARDED
 *     MEMORY: SHARED
 * }</pre>
 */
public record HybridConfig(int sharedWorkers,
                           int shardedWorkers,
                           Map<WorkloadKind, RouteTarget> routing) {

    /** Where a given workload kind is dispatched. */
    public enum RouteTarget {
        SHARED, SHARDED;

        public static RouteTarget parse(String raw) {
            if (raw == null) {
                throw new IllegalArgumentException(
                        "hybrid.routing target must not be null (allowed: SHARED|SHARDED)");
            }
            String v = raw.trim().toUpperCase();
            return switch (v) {
                case "SHARED"  -> SHARED;
                case "SHARDED" -> SHARDED;
                default -> throw new IllegalArgumentException(
                        "Unknown hybrid.routing target: '" + raw + "' (allowed: SHARED|SHARDED)");
            };
        }
    }

    public HybridConfig {
        // Defensive copy + immutability guarantee for the routing map.
        if (routing != null) {
            EnumMap<WorkloadKind, RouteTarget> copy = new EnumMap<>(WorkloadKind.class);
            copy.putAll(routing);
            routing = Collections.unmodifiableMap(copy);
        }
    }

    /**
     * Fail-fast validation. Every {@link WorkloadKind} must be present
     * in {@code routing} — there are no implicit defaults.
     */
    public void validate() {
        if (sharedWorkers <= 0) {
            throw new IllegalArgumentException(
                    "hybrid.sharedWorkers must be > 0 (got " + sharedWorkers + ")");
        }
        if (shardedWorkers <= 0) {
            throw new IllegalArgumentException(
                    "hybrid.shardedWorkers must be > 0 (got " + shardedWorkers + ")");
        }
        if (routing == null || routing.isEmpty()) {
            throw new IllegalArgumentException(
                    "hybrid.routing is required and must specify a target for every WorkloadKind "
                            + "(CPU, MEMORY). Allowed targets: SHARED|SHARDED.");
        }
        for (WorkloadKind k : WorkloadKind.values()) {
            if (!routing.containsKey(k)) {
                throw new IllegalArgumentException(
                        "hybrid.routing is missing an entry for WorkloadKind " + k
                                + ". Every kind must be explicitly routed (no implicit defaults). "
                                + "Add: " + k.name() + ": SHARED  (or SHARDED)");
            }
        }
    }

    /**
     * Returns the configured target for {@code kind}. Throws if no
     * mapping exists — callers should have run {@link #validate()} at
     * load time, so this should never trip in steady state.
     */
    public RouteTarget routeFor(WorkloadKind kind) {
        RouteTarget t = routing.get(kind);
        if (t == null) {
            throw new IllegalStateException(
                    "No hybrid.routing entry for kind " + kind
                            + " (validate() should have caught this at load time)");
        }
        return t;
    }

    /** Compact, human-readable, CSV-safe policy string, e.g.
     *  {@code "CPU=SHARDED,MEMORY=SHARED"}. */
    public String policyDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("CPU=").append(routing.get(WorkloadKind.CPU));
        sb.append(",MEMORY=").append(routing.get(WorkloadKind.MEMORY));
        return sb.toString();
    }

    @Override public String toString() {
        return "HybridConfig{sharedWorkers=" + sharedWorkers
                + ", shardedWorkers=" + shardedWorkers
                + ", routing=" + policyDescription() + "}";
    }
}

