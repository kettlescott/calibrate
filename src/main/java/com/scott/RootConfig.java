package com.scott;

import java.util.List;
import java.util.Map;

public record RootConfig(
        GlobalConfig global,
        Map<String, WorkloadConfig> workloads,
        HybridConfig hybrid,
        DynamicHybridConfig dynamicHybrid,
        DiagnosticsConfig diagnostics,
        Map<String, PhasedWorkloadEntry> hybridworkloads,
        List<RunConfig> runs
) {
    /** Returns diagnostics config, never null (disabled when YAML omits it). */
    public DiagnosticsConfig diagnosticsOrDisabled() {
        return diagnostics == null ? DiagnosticsConfig.disabled() : diagnostics;
    }
    public void validate() {
        if (global == null) {
            throw new IllegalArgumentException("global section is required");
        }
        global.validate();

        boolean hybridMode = global.hybrid();
        if (hybridMode) {
            if (hybridworkloads == null || hybridworkloads.isEmpty()) {
                throw new IllegalArgumentException("hybridworkloads section is required when global.hybrid=true");
            }
            for (Map.Entry<String, PhasedWorkloadEntry> entry : hybridworkloads.entrySet()) {
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException("hybridworkloads." + entry.getKey() + " must not be null");
                }
            }
        } else {
            if (workloads == null || workloads.isEmpty()) {
                throw new IllegalArgumentException("workloads section is required and must not be empty");
            }
            workloads.forEach((name, cfg) -> cfg.validate(name));
        }


        if (hybrid != null) {
            hybrid.validate();
        }

        if (runs == null || runs.isEmpty()) {
            throw new IllegalArgumentException("runs section is required and must not be empty");
        }

        for (RunConfig run : runs) {
            run.validate();
            if (hybridMode) {
                PhasedWorkloadEntry phased = hybridworkloads == null ? null : hybridworkloads.get(run.workload());
                if (phased == null) {
                    throw new IllegalArgumentException(
                            "Run '" + run.name() + "' references unknown hybrid workload '" + run.workload() + "'");
                }
            } else if (!workloads.containsKey(run.workload())) {
                throw new IllegalArgumentException(
                        "Run '" + run.name() + "' references unknown workload '" + run.workload() + "'");
            }

            BenchmarkMode m = BenchmarkMode.fromConfigValue(run.mode());
            if (!hybridMode && m == BenchmarkMode.HYBRID) {
                HybridConfig effective = run.hybrid() != null ? run.hybrid() : hybrid;
                if (effective == null) {
                    throw new IllegalArgumentException(
                            "Run '" + run.name() + "' uses mode=hybrid but no hybrid config was provided. "
                                    + "Define a top-level 'hybrid:' section, or a per-run 'hybrid:' override, "
                                    + "with sharedWorkers, shardedWorkers, and an explicit routing for "
                                    + "every WorkloadKind (CPU, MEMORY).");
                }
                effective.validate();
            }
            if (m == BenchmarkMode.DYNAMIC_HYBRID) {
                if (dynamicHybrid == null) {
                    throw new IllegalArgumentException(
                            "Run '" + run.name() + "' uses mode=dynamic_hybrid but no 'dynamicHybrid:' section was provided. "
                                    + "Add a top-level 'dynamicHybrid:' block with crossoverThresholdMicros, "
                                    + "minShardedWorkers, ewmaAlpha, scaleOutThresholdMicros, "
                                    + "scaleInThresholdMicros, and controllerIntervalMicros.");
                }
                dynamicHybrid.validate(global.workerCount());
            }
        }
    }
}
