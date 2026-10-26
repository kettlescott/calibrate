package com.scott;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BenchmarkConfigLoader {

    private BenchmarkConfigLoader() {
    }

    @SuppressWarnings("unchecked")
    public static RootConfig load(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Config file not found: " + path);
        }

        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(path)) {
            Object rootObj = yaml.load(in);
            if (!(rootObj instanceof Map<?, ?> rootMap)) {
                throw new IllegalArgumentException("YAML root must be a map/object");
            }

            Map<String, Object> root = (Map<String, Object>) rootMap;

            GlobalConfig global = parseGlobal((Map<String, Object>) root.get("global"));
            Map<String, WorkloadConfig> workloads = parseWorkloads((Map<String, Object>) root.get("workloads"));
            HybridConfig hybrid = parseHybrid((Map<String, Object>) root.get("hybrid"), "hybrid");
            DynamicHybridConfig dynamicHybrid = parseDynamicHybrid(
                    (Map<String, Object>) root.get("dynamicHybrid"), "dynamicHybrid");
            DiagnosticsConfig diagnostics = parseDiagnostics((Map<String, Object>) root.get("diagnostics"));
            Map<String, PhasedWorkloadEntry> hybridworkloads = parseHybridworkloads(
                    (Map<String, Object>) root.get("hybridworkloads"));
            List<RunConfig> runs = parseRuns((List<Object>) root.get("runs"));

            RootConfig config = new RootConfig(global, workloads, hybrid, dynamicHybrid,
                    diagnostics, hybridworkloads, runs);
            config.validate();
            return config;
        }
    }

    private static GlobalConfig parseGlobal(Map<String, Object> map) {
        if (map == null) {
            throw new IllegalArgumentException("global section is required");
        }
        int workerCount = intVal(map, "workerCount", Runtime.getRuntime().availableProcessors());
        int maxInflight = intVal(map, "maxInflight", workerCount * 2);
        long seed = longVal(map, "seed", 0xDEADBEEFL);
        if (map.containsKey("targetTaskNanos")) {
            System.err.println("[yaml] global.targetTaskNanos is deprecated and ignored — "
                    + "task sizing is per-entry via WorkloadEntry.targetMillis.");
        }
        int warmupSeconds = intVal(map, "warmupSeconds", 3);
        int measurementSeconds = intVal(map, "measurementSeconds", 10);
        int taskCount = intVal(map, "taskCount", 0);

        // ---- New artifact-isolation knobs (all optional; defaults
        // preserve legacy behaviour). ----
        ShardedRoutingConfig routing = ShardedRoutingConfig.defaults();
        Object routingRaw = map.get("shardedRouting");
        if (routingRaw instanceof Map<?, ?> rm) {
            @SuppressWarnings("unchecked")
            Map<String, Object> rMap = (Map<String, Object>) rm;
            ShardedRoutingConfig.Mode rmode = ShardedRoutingConfig.parseMode(
                    rMap.containsKey("mode") ? String.valueOf(rMap.get("mode")) : null);
            long routingSeed = longVal(rMap, "routingSeed", 0L);
            routing = new ShardedRoutingConfig(rmode, routingSeed);
        }
        WorkloadSeedMode workloadSeedMode = WorkloadSeedMode.parse(
                map.containsKey("workloadSeedMode") ? String.valueOf(map.get("workloadSeedMode")) : null);
        long workloadSeed = longVal(map, "workloadSeed", 0L);
        BlackholeMode blackholeMode = BlackholeMode.parse(
                map.containsKey("blackholeMode") ? String.valueOf(map.get("blackholeMode")) : null);
        boolean retainCompletedTasks = boolVal(map, "retainCompletedTasks", false);

        // --- Experiment 1: shared-queue contention (default 1 = legacy) ---
        int sharedQueueCount = intVal(map, "sharedQueueCount", 1);

        // --- Experiment 2: shard-imbalance (default null = disabled/legacy) ---
        ShardImbalanceConfig shardImbalance = null;
        Object siRaw = map.get("shardImbalance");
        if (siRaw instanceof Map<?, ?> sim) {
            @SuppressWarnings("unchecked")
            Map<String, Object> siMap = (Map<String, Object>) sim;
            double alpha = doubleVal(siMap, "alpha", 0.0);
            int hot = intVal(siMap, "hotShardId", 0);
            long rseed = longVal(siMap, "randomSeed", 12345L);
            shardImbalance = new ShardImbalanceConfig(alpha, hot, rseed);
        }

        // --- Phased experiment mode (default false) ---
        boolean hybrid = boolVal(map, "hybrid", false);

        return new GlobalConfig(
                workerCount,
                maxInflight,
                seed,
                warmupSeconds,
                measurementSeconds,
                taskCount,
                routing,
                workloadSeedMode,
                workloadSeed,
                blackholeMode,
                retainCompletedTasks,
                sharedQueueCount,
                shardImbalance,
                hybrid
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, WorkloadConfig> parseWorkloads(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("workloads section is required");
        }

        Map<String, WorkloadConfig> workloads = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            String path = "workloads." + name;

            List<WorkloadEntry> entries;

            // -- Only shape: list with one {kind, durationFile, ratio=1.0, name?} entry --
            if (value instanceof List<?> list) {
                entries = new ArrayList<>(list.size());
                for (int i = 0; i < list.size(); i++) {
                    entries.add(WorkloadEntry.fromMap(
                            list.get(i), path + "[" + i + "]"));
                }
                if (entries.size() != 1) {
                    throw new IllegalArgumentException(
                            path + " must contain exactly one entry (durationFile mode only)");
                }
            }
            // -- Map-shaped shorthand: {kind, durationFile, ratio?} --
            else if (value instanceof Map<?, ?> wmRaw) {
                Map<String, Object> wm = (Map<String, Object>) wmRaw;
                entries = parseWorkloadMap(name, wm);
            }
            else {
                throw new IllegalArgumentException(
                        path + " must be a list of entries or a map");
            }

            workloads.put(name, new WorkloadConfig(entries));
        }
        return workloads;
    }

    /** Parses a map-shaped workload shorthand: {@code {kind, durationFile, ratio?}}. */
    @SuppressWarnings("unchecked")
    private static List<WorkloadEntry> parseWorkloadMap(String name, Map<String, Object> wm) {
        String path = "workloads." + name;

        Object kindRaw = wm.get("kind");
        if (kindRaw == null || !isResourceKind(String.valueOf(kindRaw))) {
            throw new IllegalArgumentException(
                    path + ": unsupported workload shape. Use {kind: CPU|MEMORY, durationFile: <path>, ratio: 1.0}.");
        }
        return List.of(WorkloadEntry.fromMap(wm, path));
    }

    private static boolean isResourceKind(String s) {
        if (s == null) return false;
        String v = s.trim().toUpperCase();
        return "CPU".equals(v) || "MEMORY".equals(v);
    }


    @SuppressWarnings("unchecked")
    private static List<RunConfig> parseRuns(List<Object> list) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("runs section is required");
        }
        List<RunConfig> runs = new ArrayList<>();
        for (Object obj : list) {
            if (!(obj instanceof Map<?, ?> runMap)) {
                throw new IllegalArgumentException("Each runs[] item must be a map");
            }
            Map<String, Object> rm = (Map<String, Object>) runMap;
            String name = strVal(rm, "name", null);
            HybridConfig perRunHybrid = parseHybrid(
                    (Map<String, Object>) rm.get("hybrid"),
                    "runs[" + name + "].hybrid");
            // Per-run on/off switch. Defaults to true so existing
            // configs without the field continue to run unchanged.
            boolean enabled = boolVal(rm, "enabled", true);
            runs.add(new RunConfig(
                    name,
                    strVal(rm, "mode", null),
                    strVal(rm, "workload", null),
                    perRunHybrid,
                    enabled
            ));
        }
        return runs;
    }


    /**
     * Parses a {@code hybrid:} block. Returns {@code null} when absent.
     * Performs no defaulting on routing — every WorkloadKind must be
     * specified explicitly (validated by {@link HybridConfig#validate()}).
     *
     * <pre>{@code
     * hybrid:
     *   sharedWorkers: 8
     *   shardedWorkers: 8
     *   routing:
     *     CPU:    SHARDED
     *     MEMORY: SHARED
     * }</pre>
     */
    @SuppressWarnings("unchecked")
    private static HybridConfig parseHybrid(Map<String, Object> map, String path) {
        if (map == null) return null;

        if (!map.containsKey("sharedWorkers")) {
            throw new IllegalArgumentException(path + ".sharedWorkers is required");
        }
        if (!map.containsKey("shardedWorkers")) {
            throw new IllegalArgumentException(path + ".shardedWorkers is required");
        }
        int sharedWorkers  = intVal(map, "sharedWorkers", 0);
        int shardedWorkers = intVal(map, "shardedWorkers", 0);

        Object routingRaw = map.get("routing");
        if (!(routingRaw instanceof Map<?, ?> routingMap)) {
            throw new IllegalArgumentException(
                    path + ".routing is required and must be a map of WorkloadKind -> SHARED|SHARDED. "
                            + "Every kind (CPU, MEMORY) must be specified explicitly — there are no defaults.");
        }

        java.util.EnumMap<WorkloadKind, HybridConfig.RouteTarget> routing =
                new java.util.EnumMap<>(WorkloadKind.class);
        for (Map.Entry<?, ?> e : routingMap.entrySet()) {
            String kindStr = String.valueOf(e.getKey()).trim().toUpperCase();
            WorkloadKind kind;
            try {
                kind = WorkloadKind.valueOf(kindStr);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                        path + ".routing has unknown WorkloadKind '" + e.getKey()
                                + "' (allowed: CPU, MEMORY)");
            }
            HybridConfig.RouteTarget target = HybridConfig.RouteTarget.parse(String.valueOf(e.getValue()));
            routing.put(kind, target);
        }

        return new HybridConfig(sharedWorkers, shardedWorkers, routing);
    }

    /**
     * Parses a {@code dynamicHybrid:} block. Returns {@code null} when
     * absent — existing SHARED / SHARDED / HYBRID configs therefore
     * remain byte-identical to a build without this code. Only when a
     * run selects {@code mode: dynamic_hybrid} does the presence of
     * this block become mandatory (enforced by
     * {@link RootConfig#validate()}).
     */
    private static DynamicHybridConfig parseDynamicHybrid(Map<String, Object> map, String path) {
        if (map == null) return null;
        if (!map.containsKey("crossoverThresholdMicros")) {
            throw new IllegalArgumentException(path + ".crossoverThresholdMicros is required");
        }
        if (!map.containsKey("minShardedWorkers")) {
            throw new IllegalArgumentException(path + ".minShardedWorkers is required");
        }
        if (!map.containsKey("ewmaAlpha")) {
            throw new IllegalArgumentException(path + ".ewmaAlpha is required");
        }
        if (!map.containsKey("scaleOutThresholdMicros")) {
            throw new IllegalArgumentException(path + ".scaleOutThresholdMicros is required");
        }
        if (!map.containsKey("scaleInThresholdMicros")) {
            throw new IllegalArgumentException(path + ".scaleInThresholdMicros is required");
        }
        if (!map.containsKey("controllerIntervalMicros")) {
            throw new IllegalArgumentException(path + ".controllerIntervalMicros is required");
        }
        return new DynamicHybridConfig(
                longVal(map,   "crossoverThresholdMicros", 0L),
                intVal(map,    "minShardedWorkers",        0),
                // Default 1 preserves existing YAML that predates this
                // field. Validation still enforces >= 1.
                intVal(map,    "minSharedWorkers",         1),
                doubleVal(map, "ewmaAlpha",                0.2),
                longVal(map,   "scaleOutThresholdMicros",  0L),
                longVal(map,   "scaleInThresholdMicros",   0L),
                longVal(map,   "controllerIntervalMicros", 0L));
    }

    /**
     * Parses the optional {@code diagnostics:} block. Returns
     * {@link DiagnosticsConfig#disabled()} when the block is missing —
     * benchmark behaviour is then byte-identical to a build without
     * this code.
     */
    private static DiagnosticsConfig parseDiagnostics(Map<String, Object> map) {
        if (map == null) return DiagnosticsConfig.disabled();
        // Default policy when the block exists:
        //   enabled            : false  (must be explicitly turned on)
        //   perWorkerLatency   : false  (HEAVY — see DiagnosticsConfig javadoc)
        //   queueDepthByShard  : true   (cheap; useful for sharded imbalance)
        //   windowSampling     : true   (cheap; compact per-window summaries)
        //   windowSeconds      : 1
        //   slowExecutionMicros: 200    (static threshold; no dynamic p95)
        boolean enabled            = boolVal(map, "enabled",            false);
        boolean perWorkerLatency   = boolVal(map, "perWorkerLatency",   false);
        boolean queueDepthByShard  = boolVal(map, "queueDepthByShard",  true);
        boolean windowSampling     = boolVal(map, "windowSampling",     true);
        boolean windowCorrelation  = boolVal(map, "windowCorrelation",  true);
        // Per-shard latency CSV output (SHARDED mode only). OFF by
        // default — when on, writes 3 CSV files to the run directory.
        boolean shardLatencyCsv    = boolVal(map, "shardLatencyCsv",    false);
        // Per-task CSV dump (SHARDED mode only). OFF by default —
        // heavy: writes one CSV row per measurement task.
        boolean rawTaskLogging     = boolVal(map, "rawTaskLogging",     false);
        // Streaming per-task execution-time CSV (any mode). OFF by
        // default — when on, writes results/<runName>/task_execution_times.csv
        // with one row per measurement task: kind, taskId, shardId,
        // queueWaitNanos, executionNanos, endToEndNanos.
        boolean taskExecutionCsv   = boolVal(map, "taskExecutionCsv",   false);
        // Window size for the per-shard / per-window CSV. Default 100 ms.
        long    shardWindowMillis  = longVal(map, "shardWindowMillis",  100L);
        // Accepts decimals (e.g. 0.5, 0.1) so sub-second windows are
        // possible for short diagnostic runs. Internally converted to
        // nanoseconds by DiagnosticsCollector.
        double  windowSeconds      = doubleVal(map, "windowSeconds",    1.0);
        long    slowExecutionUs    = longVal(map, "slowExecutionMicros", 200L);
        return new DiagnosticsConfig(enabled, perWorkerLatency, queueDepthByShard,
                windowSampling, windowCorrelation, shardLatencyCsv, rawTaskLogging,
                taskExecutionCsv, windowSeconds, shardWindowMillis, slowExecutionUs);
    }


    private static double doubleVal(Map<String, Object> map, String key, double defaultVal) {
        Object value = map.get(key);
        if (value == null) return defaultVal;
        if (value instanceof Number n) return n.doubleValue();
        return Double.parseDouble(String.valueOf(value));
    }

    private static int intVal(Map<String, Object> map, String key, int defaultVal) {
        Object value = map.get(key);
        if (value == null) return defaultVal;
        return Integer.parseInt(String.valueOf(value));
    }

    private static long longVal(Map<String, Object> map, String key, long defaultVal) {
        Object value = map.get(key);
        if (value == null) return defaultVal;
        return Long.parseLong(String.valueOf(value));
    }

    private static boolean boolVal(Map<String, Object> map, String key, boolean defaultVal) {
        Object value = map.get(key);
        if (value == null) return defaultVal;
        return Boolean.parseBoolean(String.valueOf(value));
    }

    private static String strVal(Map<String, Object> map, String key, String defaultVal) {
        Object value = map.get(key);
        return value == null ? defaultVal : String.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PhasedWorkloadEntry> parseHybridworkloads(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return new LinkedHashMap<>();  // Empty if not present
        }

        Map<String, PhasedWorkloadEntry> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            String path = "hybridworkloads." + name;

            if (!(value instanceof Map<?, ?>)) {
                throw new IllegalArgumentException(path + " must be a map");
            }

            Map<String, Object> entryMap = (Map<String, Object>) value;
            
            // Parse basic fields
            String kindStr = strVal(entryMap, "kind", null);
            WorkloadKind kind = kindStr != null ? WorkloadKind.valueOf(kindStr.toUpperCase())
                    : null;
            if (kind == null) {
                throw new IllegalArgumentException(path + ".kind is required");
            }

            String executorStr = strVal(entryMap, "executor", "SHARDED");
            ExecutorMode executor = ExecutorMode.parse(executorStr);

            long targetMicros = longVal(entryMap, "targetMicros", 0L);

            // Parse optional hybrid config
            AdaptiveHybridConfig adaptiveHybrid = null;
            Object hybridRaw = entryMap.get("hybrid");
            if (hybridRaw instanceof Map<?, ?> hm) {
                Map<String, Object> hMap = (Map<String, Object>) hm;
                String initialModeStr = strVal(hMap, "initialMode", "SHARDED");
                ExecutorMode initialMode = ExecutorMode.parse(initialModeStr);
                double sharedEnterCv = doubleVal(hMap, "sharedEnterCv", 0.16);
                double shardedEnterCv = doubleVal(hMap, "shardedEnterCv", 0.08);
                long windowMillis = longVal(hMap, "windowMillis", 100L);
                long debugCvPrintIntervalSeconds = longVal(hMap, "debugCvPrint", 0L);
                boolean only = boolVal(hMap, "only", false);
                // Pass targetMicros to config (for targetDeviation calculation)
                adaptiveHybrid = new AdaptiveHybridConfig(initialMode, sharedEnterCv, shardedEnterCv, 
                    windowMillis, debugCvPrintIntervalSeconds, targetMicros);
            }

            // Parse phases
            List<PhasedConfig> phases = new ArrayList<>();
            Object phasesRaw = entryMap.get("phases");
            if (phasesRaw instanceof List<?> phasesList) {
                for (int i = 0; i < phasesList.size(); i++) {
                    Object pObj = phasesList.get(i);
                    if (!(pObj instanceof Map<?, ?>)) {
                        throw new IllegalArgumentException(
                                path + ".phases[" + i + "] must be a map");
                    }
                    Map<String, Object> pMap = (Map<String, Object>) pObj;
                    int durationSeconds = intVal(pMap, "durationSeconds", 0);
                    String durationFile = strVal(pMap, "durationFile", null);
                    phases.add(new PhasedConfig(durationSeconds, durationFile));
                }
            }
            if (phases.isEmpty()) {
                throw new IllegalArgumentException(path + ".phases is required and must not be empty");
            }

            // Parse optional memory config
            MemoryWorkloadConfig memory = null;
            Object memRaw = entryMap.get("memory");
            if (memRaw instanceof Map<?, ?> mm) {
                memory = MemoryWorkloadConfig.fromMap((Map<String, Object>) mm);
            }

            if (entryMap.containsKey("cpuIterations")) {
                throw new IllegalArgumentException(path + ".cpuIterations is no longer supported");
            }
            if (entryMap.containsKey("memorySteps")) {
                throw new IllegalArgumentException(path + ".memorySteps is no longer supported");
            }

            PhasedWorkloadEntry pwe = new PhasedWorkloadEntry(
                    name, kind, executor, targetMicros, adaptiveHybrid, phases, memory,
                    0, 0);
            result.put(name, pwe);
        }
        return result;
    }
}
