package com.scott;

/**
 * Single phase definition in a phased workload.
 *
 * <p>Holds the measurement duration and the duration file path.
 * Each phase runs for exactly {@code durationSeconds}, executing tasks
 * whose individual durations are loaded from {@code durationFile}.
 *
 * <p>When the phase duration expires:
 * <ul>
 *   <li>No new tasks are submitted.</li>
 *   <li>Previously submitted tasks are allowed to complete.</li>
 *   <li>The next phase begins (or the run ends if this is the last phase).</li>
 * </ul>
 *
 * Phases are 0-indexed by their position in the phases list.
 */
public record PhasedConfig(
        int durationSeconds,
        String durationFile
) {
    public PhasedConfig {
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException(
                    "phase.durationSeconds must be > 0 (got " + durationSeconds + ")");
        }
        if (durationFile == null || durationFile.isBlank()) {
            throw new IllegalArgumentException(
                    "phase.durationFile must not be null or blank");
        }
    }

    public String summary() {
        return "PhasedConfig{duration=" + durationSeconds + "s, file=" + durationFile + "}";
    }
}

