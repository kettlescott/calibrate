package com.scott;

public record CalibrateEntry(
        WorkloadKind kind,
        String distribution,
        double startCv,
        double maxCv,
        long durationMicros
) {
    public void validate(String path) {
        if (kind == null) {
            throw new IllegalArgumentException(path + ".kind is required");
        }
        if (distribution == null || distribution.isBlank()) {
            throw new IllegalArgumentException(path + ".distribution is required");
        }
        if (startCv <= 0.0 || !Double.isFinite(startCv)) {
            throw new IllegalArgumentException(path + ".start_cv must be > 0");
        }
        if (maxCv <= 0.0 || !Double.isFinite(maxCv)) {
            throw new IllegalArgumentException(path + ".max must be > 0");
        }
        if (startCv > maxCv) {
            throw new IllegalArgumentException(path + ".start_cv must be <= max");
        }
        if (durationMicros <= 0) {
            throw new IllegalArgumentException(path + ".duration must be > 0 (microseconds)");
        }
    }
}

