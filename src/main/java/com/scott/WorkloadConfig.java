package com.scott;

import java.util.List;

/**
 * YAML-driven workload definition.
 *
 * <p>A workload is a list of {@link WorkloadEntry} entries. Each entry
 * pairs a {@link WorkloadKind} (CPU | MEMORY) with a duration file and
 * a generation ratio. This simplified benchmark supports one
 * duration-file-backed entry per workload.
 *
 * <p>Single-class workloads are represented by a one-entry list with
 * ratio {@code 1.0}.
 */
public record WorkloadConfig(List<WorkloadEntry> entries) {

    public WorkloadConfig {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException("workload must have at least one entry");
        }
        entries = List.copyOf(entries);
    }

    /** {@code true} when this workload has exactly one entry. */
    public boolean isSingle() { return entries.size() == 1; }

    public void validate(String name) {
        final String path = "workloads." + name;
        double sum = 0.0;
        for (int i = 0; i < entries.size(); i++) {
            WorkloadEntry e = entries.get(i);
            if (e.ratio() <= 0.0) {
                throw new IllegalArgumentException(
                        path + ".entries[" + i + "].ratio must be > 0");
            }
            sum += e.ratio();
        }
        if (Math.abs(sum - 1.0) > 1e-3) {
            throw new IllegalArgumentException(
                    path + ".entries[].ratio must sum to 1.0 (got " + String.format("%.4f", sum) + ")");
        }
    }
}
