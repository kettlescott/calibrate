package com.scott;

import java.util.List;

/**
 * One entry in a phased workload (hybridworkloads).
 *
 * <p>Describes a workload to be executed in phases:
 * <ul>
 *   <li>{@code kind}            — CPU | MEMORY</li>
 *   <li>{@code executor}        — SHARED | SHARDED | HYBRID</li>
 *   <li>{@code targetMicros}    — target execution time per task (µs); for record-keeping</li>
 *   <li>{@code hybrid}          — optional HYBRID-specific config (ignored if executor != HYBRID)</li>
 *   <li>{@code phases}          — list of phase definitions (durationSeconds, durationFile)</li>
 *   <li>{@code memory}          — optional MEMORY-kind config</li>
 *   <li>{@code cpuIterations}   — optional fixed CPU iterations</li>
 *   <li>{@code memorySteps}     — optional fixed MEMORY steps</li>
 * </ul>
 *
 * <p>No ratio field: phased workloads are single-entry only (ratio implicitly 1.0).
 *
 * <p>Each phase's durationFile determines per-task duration at runtime.
 */
public record PhasedWorkloadEntry(
        String name,
        WorkloadKind kind,
        ExecutorMode executor,
        long targetMicros,
        AdaptiveHybridConfig hybrid,
        List<PhasedConfig> phases,
        MemoryWorkloadConfig memory,
        int cpuIterations,
        int memorySteps
) {
    public PhasedWorkloadEntry {
        if (kind == null) {
            throw new IllegalArgumentException("phased entry: kind is required");
        }
        if (executor == null) {
            throw new IllegalArgumentException("phased entry: executor is required");
        }
        if (phases == null || phases.isEmpty()) {
            throw new IllegalArgumentException("phased entry: phases must not be empty");
        }

        // Validate hybrid config presence
        if (executor == ExecutorMode.HYBRID && hybrid == null) {
            throw new IllegalArgumentException(
                    "phased entry: executor=HYBRID requires hybrid config");
        }
        if (executor != ExecutorMode.HYBRID && hybrid != null) {
            throw new IllegalArgumentException(
                    "phased entry: hybrid config is only valid when executor=HYBRID");
        }

        // CPU/MEMORY specific validation
        if (cpuIterations > 0 && kind != WorkloadKind.CPU) {
            throw new IllegalArgumentException("cpuIterations is only valid for CPU workloads");
        }
        if (memorySteps > 0 && kind != WorkloadKind.MEMORY) {
            throw new IllegalArgumentException("memorySteps is only valid for MEMORY workloads");
        }
    }

    public String displayName() {
        return name != null && !name.isBlank() ? name : kind.name();
    }

    public String summary() {
        return "PhasedWorkloadEntry{name=" + displayName() + ", kind=" + kind
                + ", executor=" + executor + ", targetMicros=" + targetMicros
                + ", phases.size=" + phases.size() + "}";
    }

    /**
     * Returns the MEMORY config, falling back to defaults when null.
     */
    public MemoryWorkloadConfig memoryOrDefaults() {
        return memory != null ? memory : MemoryWorkloadConfig.defaults();
    }
}

