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

public final class CalibrateConfigLoader {

    private CalibrateConfigLoader() {
    }

    @SuppressWarnings("unchecked")
    public static CalibrateConfig load(Path path) throws IOException {
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
            Map<String, Object> globalMap = (Map<String, Object>) root.get("global");
            GlobalConfig global = parseGlobal(globalMap);
            Map<String, List<CalibrateEntry>> calibrate = parseCalibrate((Map<String, Object>) root.get("calibrate"));
            int binarySearchRepeats = intVal(globalMap, "binarySearchRepeats", 1);
            double binaryCvEps = doubleVal(globalMap, "BINARY_CV_EPSILON",
                    doubleVal(globalMap, "BINARY_CV_EPS",
                            doubleVal(globalMap, "binaryCvEps", 1e-7)));
            CalibrateConfig config = new CalibrateConfig(global, calibrate, binarySearchRepeats, binaryCvEps);
            config.validate();
            return config;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<CalibrateEntry>> parseCalibrate(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            throw new IllegalArgumentException("calibrate section is required");
        }
        Map<String, List<CalibrateEntry>> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String workloadName = e.getKey();
            if (!(e.getValue() instanceof List<?> list)) {
                throw new IllegalArgumentException("calibrate." + workloadName + " must be a list");
            }
            List<CalibrateEntry> entries = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                Object raw = list.get(i);
                if (!(raw instanceof Map<?, ?> rawMap)) {
                    throw new IllegalArgumentException("calibrate." + workloadName + "[" + i + "] must be a map");
                }
                Map<String, Object> m = (Map<String, Object>) rawMap;
                WorkloadKind kind = WorkloadKind.fromLabel(strVal(m, "kind", null));
                String distribution = strVal(m, "distribution", null);
                double startCv = doubleVal(m, "start_cv", Double.NaN);
                double maxCv = doubleVal(m, "max", Double.NaN);
                long durationMicros = longVal(m, "duration", 0L);
                entries.add(new CalibrateEntry(kind, distribution, startCv, maxCv, durationMicros));
            }
            out.put(workloadName, List.copyOf(entries));
        }
        return out;
    }

    private static GlobalConfig parseGlobal(Map<String, Object> map) {
        if (map == null) {
            throw new IllegalArgumentException("global section is required");
        }
        int workerCount = intVal(map, "workerCount", Runtime.getRuntime().availableProcessors());
        int maxInflight = intVal(map, "maxInflight", workerCount * 2);
        long seed = longVal(map, "seed", 0xDEADBEEFL);
        int warmupSeconds = intVal(map, "warmupSeconds", 0);
        int measurementSeconds = intVal(map, "measurementSeconds", 20);
        int taskCount = intVal(map, "taskCount", 0);
        return new GlobalConfig(workerCount, maxInflight, seed, warmupSeconds, measurementSeconds, taskCount);
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

    private static String strVal(Map<String, Object> map, String key, String defaultVal) {
        Object value = map.get(key);
        return value == null ? defaultVal : String.valueOf(value);
    }
}

