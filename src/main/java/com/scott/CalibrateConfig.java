package com.scott;

import java.util.List;
import java.util.Map;

public record CalibrateConfig(
        GlobalConfig global,
        Map<String, List<CalibrateEntry>> calibrate,
        int binarySearchRepeats,
        double binaryCvEps
) {
    public void validate() {
        if (global == null) {
            throw new IllegalArgumentException("global section is required");
        }
        global.validate();
        if (binarySearchRepeats <= 0) {
            throw new IllegalArgumentException("global.binarySearchRepeats must be > 0");
        }
        if (!Double.isFinite(binaryCvEps) || binaryCvEps <= 0.0) {
            throw new IllegalArgumentException("global.BINARY_CV_EPS must be > 0");
        }
        if (calibrate == null || calibrate.isEmpty()) {
            throw new IllegalArgumentException("calibrate section is required and must not be empty");
        }
        for (Map.Entry<String, List<CalibrateEntry>> e : calibrate.entrySet()) {
            if (e.getValue() == null || e.getValue().isEmpty()) {
                throw new IllegalArgumentException("calibrate." + e.getKey() + " must contain at least one entry");
            }
            for (CalibrateEntry entry : e.getValue()) {
                entry.validate("calibrate." + e.getKey());
            }
        }
    }
}

