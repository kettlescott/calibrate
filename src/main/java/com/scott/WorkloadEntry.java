package com.scott;

import java.util.Map;

/**
 * One entry of a workload mix.
 *
 * <p>Describes a single resource-shaped task class:
 * <ul>
 *   <li>{@code kind}         — CPU | MEMORY</li>
 *   <li>{@code targetMillis} — desired wall-clock execution time per task (milliseconds)</li>
 *   <li>{@code targetMicros} — alternative to targetMillis for finer granularity (microseconds);
 *       if both are set, targetMicros takes precedence for MEMORY duration-controlled mode</li>
 *   <li>{@code ratio}        — fraction of generated tasks (entries within
 *       a workload sum to ≈ 1.0)</li>
 *   <li>{@code name}         — optional human-readable label for output</li>
 *   <li>{@code memory}       — optional MEMORY-kind config (access pattern,
 *       buffer size, writeBack, memoryMode); ignored unless {@code kind == MEMORY}</li>
 * </ul>
 */
public record WorkloadEntry(
        String name,
        WorkloadKind kind,
        long targetMillis,
        double ratio,
        MemoryWorkloadConfig memory,
        int cpuIterations,
        int memorySteps,
        long targetMicros,
        String durationFile
) {

    /** Backwards-compatible constructor: no MEMORY config, no fixed cpuIterations. */
    public WorkloadEntry(String name, WorkloadKind kind, long targetMillis, double ratio) {
        this(name, kind, targetMillis, ratio, null, 0, 0, 0, null);
    }

    /** Backwards-compatible constructor: explicit MEMORY config, no fixed cpuIterations. */
    public WorkloadEntry(String name, WorkloadKind kind, long targetMillis, double ratio,
                         MemoryWorkloadConfig memory) {
        this(name, kind, targetMillis, ratio, memory, 0, 0, 0, null);
    }

    /** Backwards-compatible constructor: pre-memorySteps callers. */
    public WorkloadEntry(String name, WorkloadKind kind, long targetMillis, double ratio,
                         MemoryWorkloadConfig memory, int cpuIterations) {
        this(name, kind, targetMillis, ratio, memory, cpuIterations, 0, 0, null);
    }


    public WorkloadEntry {
        if (kind == null) {
            throw new IllegalArgumentException("workload entry: kind is required");
        }
        if (kind != WorkloadKind.CPU && kind != WorkloadKind.MEMORY) {
            throw new IllegalArgumentException("workload entry: only CPU and MEMORY are supported");
        }

        // Duration-file-only mode.
        boolean hasFile = durationFile != null && !durationFile.isBlank();
        if (!hasFile) {
            throw new IllegalArgumentException("workload entry: durationFile is required");
        }
        if (hasFile) {
            if (ratio != 1.0) {
                throw new IllegalArgumentException(
                        "workload entry: when durationFile is used, ratio must be exactly 1.0 (got " + ratio + ")");
            }
            if (targetMicros > 0) {
                throw new IllegalArgumentException(
                        "workload entry: durationFile and targetMicros are mutually exclusive");
            }
            if (targetMillis > 0) {
                throw new IllegalArgumentException(
                        "workload entry: durationFile and targetMillis are mutually exclusive");
            }
            if (cpuIterations > 0) {
                throw new IllegalArgumentException(
                        "workload entry: durationFile and cpuIterations are mutually exclusive");
            }
            if (memorySteps > 0) {
                throw new IllegalArgumentException(
                        "workload entry: durationFile and memorySteps are mutually exclusive");
            }
        }
        if (ratio <= 0.0 || !Double.isFinite(ratio)) {
            throw new IllegalArgumentException("workload entry: ratio must be > 0");
        }
    }

    /** True when this entry runs a fixed iteration count (no calibration). */
    public boolean usesFixedCpuIterations() {
        return kind == WorkloadKind.CPU && cpuIterations > 0;
    }

    /** True when this entry runs a fixed memory step count (no calibration). */
    public boolean usesFixedMemorySteps() {
        return kind == WorkloadKind.MEMORY && memorySteps > 0;
    }

    public String displayName() {
        if (name != null && !name.isBlank()) return name;
        return kind.name().toLowerCase() + "_" + targetMillis + "ms";
    }

    /** Returns true if this entry uses durationFile mode. */
    public boolean usesDurationFile() {
        return durationFile != null && !durationFile.isBlank();
    }

    /** Returns the MEMORY config, falling back to defaults when null. */
    public MemoryWorkloadConfig memoryOrDefaults() {
        return memory != null ? memory : MemoryWorkloadConfig.defaults();
    }

    @SuppressWarnings("unchecked")
    public static WorkloadEntry fromMap(Object raw, String path) {
        if (!(raw instanceof Map<?, ?> m)) {
            throw new IllegalArgumentException(path + " must be a map with {kind,durationFile,ratio}");
        }
        Map<String, Object> em = (Map<String, Object>) m;
        String name = em.get("name") == null ? null : String.valueOf(em.get("name"));
        Object kindRaw = em.get("kind");
        if (kindRaw == null) {
            throw new IllegalArgumentException(path + ".kind is required (CPU|MEMORY)");
        }
        WorkloadKind kind = WorkloadKind.fromLabel(String.valueOf(kindRaw));

        // Enforce simplified schema upfront so error messages are explicit.
        if (em.containsKey("targetMillis")) {
            throw new IllegalArgumentException(path + ".targetMillis is no longer supported; use durationFile");
        }
        if (em.containsKey("targetMicros")) {
            throw new IllegalArgumentException(path + ".targetMicros is no longer supported; use durationFile");
        }
        if (em.containsKey("cpuIterations")) {
            throw new IllegalArgumentException(path + ".cpuIterations is no longer supported; use durationFile");
        }
        if (em.containsKey("memorySteps")) {
            throw new IllegalArgumentException(path + ".memorySteps is no longer supported; use durationFile");
        }

        // Optional fixed-iteration count. CPU-only validity is enforced
        // by the record's compact constructor below.
        Object ciRaw = em.get("cpuIterations");
        int cpuIterations = ciRaw == null ? 0 : Integer.parseInt(String.valueOf(ciRaw));

        // Optional fixed memory step count. MEMORY-only validity is
        // enforced by the record's compact constructor below.
        Object msRaw = em.get("memorySteps");
        int memorySteps = msRaw == null ? 0 : Integer.parseInt(String.valueOf(msRaw));

        // Optional targetMicros for fine-grained MEMORY duration control
        Object tmicro = em.get("targetMicros");
        long targetMicros = tmicro == null ? 0L : Long.parseLong(String.valueOf(tmicro));

        // Optional durationFile for file-based task durations
        Object durFileRaw = em.get("durationFile");
        String durationFile = durFileRaw == null ? null : String.valueOf(durFileRaw);

        long targetMillis = 0L;
        if (durationFile == null || durationFile.isBlank()) {
            throw new IllegalArgumentException(path + ".durationFile is required");
        }

        Object r = em.get("ratio");
        double ratio = r == null ? 1.0 : Double.parseDouble(String.valueOf(r));

        // Optional MEMORY block: only consumed when kind == MEMORY.
        MemoryWorkloadConfig memory = null;
        Object memRaw = em.get("memory");
        if (memRaw != null) {
            if (kind != WorkloadKind.MEMORY) {
                System.err.printf("[yaml] %s.memory ignored: only valid when kind=MEMORY%n", path);
            } else if (memRaw instanceof Map<?, ?> mm) {
                memory = parseMemory((Map<String, Object>) mm, path + ".memory");
            } else {
                throw new IllegalArgumentException(path + ".memory must be a map");
            }
        }

        return new WorkloadEntry(name, kind, targetMillis, ratio, memory, cpuIterations, memorySteps, targetMicros, durationFile);
    }

    private static MemoryWorkloadConfig parseMemory(Map<String, Object> mm, String path) {
        Object apRaw = mm.get("accessPattern");
        MemoryBoundWorkload.AccessPattern pattern = apRaw == null
                ? MemoryWorkloadConfig.DEFAULT_PATTERN
                : MemoryBoundWorkload.parsePattern(String.valueOf(apRaw));
        Object bmRaw = mm.get("bufferMB");
        int bufferMB = bmRaw == null
                ? MemoryWorkloadConfig.DEFAULT_BUFFER_MB
                : Integer.parseInt(String.valueOf(bmRaw));
        Object wbRaw = mm.get("writeBack");
        boolean writeBack = wbRaw == null
                ? MemoryWorkloadConfig.DEFAULT_WRITE_BACK
                : Boolean.parseBoolean(String.valueOf(wbRaw));
        return new MemoryWorkloadConfig(pattern, bufferMB, writeBack);
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append("name=").append(displayName())
          .append(", kind=").append(kind.name())
          .append(", targetMillis=").append(targetMillis)
          .append(", ratio=").append(String.format("%.4f", ratio));
        if (usesDurationFile()) {
            sb.append(", durationFile=").append(durationFile);
        }
        if (kind == WorkloadKind.MEMORY) {
            sb.append(", ").append(memoryOrDefaults().summary());
        }
        return sb.toString();
    }
}
