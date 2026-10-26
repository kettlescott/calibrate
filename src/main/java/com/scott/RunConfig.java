package com.scott;

public record RunConfig(
        String name,
        String mode,
        String workload,
        HybridConfig hybrid,
        boolean enabled
) {
    /** Convenience constructor: no per-run hybrid override. Enabled by default. */
    public RunConfig(String name, String mode, String workload) {
        this(name, mode, workload, null, true);
    }

    /** Convenience constructor: per-run hybrid override. Enabled by default. */
    public RunConfig(String name, String mode, String workload, HybridConfig hybrid) {
        this(name, mode, workload, hybrid, true);
    }

    public void validate() {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("runs[].name is required");
        }
        if (mode == null || mode.isBlank()) {
            throw new IllegalArgumentException("runs[" + name + "].mode is required");
        }
        if (workload == null || workload.isBlank()) {
            throw new IllegalArgumentException("runs[" + name + "].workload is required");
        }
        BenchmarkMode parsed = BenchmarkMode.fromConfigValue(mode);
        if (parsed != BenchmarkMode.SHARED
                && parsed != BenchmarkMode.SHARDED
                && parsed != BenchmarkMode.HYBRID
                && parsed != BenchmarkMode.DYNAMIC_HYBRID) {
            throw new IllegalArgumentException(
                    "runs[" + name + "].mode must be 'shared', 'sharded', 'hybrid', or 'dynamic_hybrid', got '" + mode + "'");
        }
        if (hybrid != null) {
            hybrid.validate();
        }
    }
}
