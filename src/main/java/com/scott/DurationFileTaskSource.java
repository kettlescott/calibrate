package com.scott;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Reads task durations (in microseconds) from a file cyclically.
 * Format: one duration per line, in microseconds (long). Skips empty lines and lines starting with #.
 * Cycles back to start when EOF is reached using modulo.
 * Thread-safe with synchronized access to nextDurationMicros().
 */
public final class DurationFileTaskSource {
    private final List<Long> durations;
    private int nextIndex;

    public DurationFileTaskSource(String filePath) throws IOException {
        this(Paths.get(filePath));
    }

    public DurationFileTaskSource(Path filePath) throws IOException {
        this.durations = new ArrayList<>();
        this.nextIndex = 0;
        readDurations(filePath);
    }

    public DurationFileTaskSource(Collection<Long> durations) {
        this.durations = new ArrayList<>();
        this.nextIndex = 0;
        if (durations == null || durations.isEmpty()) {
            throw new IllegalArgumentException("Duration list must not be empty");
        }
        for (Long v : durations) {
            if (v == null || v <= 0) {
                throw new IllegalArgumentException("Duration list contains non-positive value: " + v);
            }
            this.durations.add(v);
        }
    }

    private void readDurations(Path filePath) throws IOException {
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("Duration file not found: " + filePath.toAbsolutePath());
        }
        List<String> lines = Files.readAllLines(filePath);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Duration file is empty: " + filePath.toAbsolutePath());
        }

        for (int lineNum = 1; lineNum <= lines.size(); lineNum++) {
            String line = lines.get(lineNum - 1).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            try {
                long micros = Long.parseLong(line);
                if (micros <= 0) {
                    throw new IllegalArgumentException(
                            "Duration must be > 0 at line " + lineNum + ": " + line);
                }
                durations.add(micros);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid duration at line " + lineNum + " (expected positive long): " + line, e);
            }
        }

        if (durations.isEmpty()) {
            throw new IllegalArgumentException(
                    "Duration file contains no valid durations (only comments/empty lines): " + filePath.toAbsolutePath());
        }
    }

    /** Returns next duration in µs, cycling back to start when EOF is reached. */
    public synchronized long nextDurationMicros() {
        long result = durations.get(nextIndex);
        nextIndex = (nextIndex + 1) % durations.size();
        return result;
    }

    /** Peeks at next duration without consuming it. */
    public synchronized long peekNextDurationMicros() {
        return durations.get(nextIndex);
    }

    public int totalTasks() {
        return durations.size();
    }

    public int tasksRead() {
        return nextIndex;
    }

    public synchronized void reset() {
        nextIndex = 0;
    }
}

