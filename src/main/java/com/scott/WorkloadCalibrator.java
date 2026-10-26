package com.scott;

/**
 * Calibrates iteration / step counts for {@link CpuBoundWorkload} and
 * {@link MemoryBoundWorkload} so that one {@code execute()} call takes
 * approximately a desired wall-clock duration.
 *
 * <h3>Algorithm (both kinds)</h3>
 * <ol>
 *   <li>Run a small <em>pilot</em> to measure cost-per-unit on the
 *       current hardware.</li>
 *   <li>Linearly extrapolate the unit count for the requested target.</li>
 *   <li>Run a <em>verification</em> pass and apply a single correction
 *       factor to compensate for JIT warm-up / frequency scaling.</li>
 * </ol>
 *
 * <p>Deterministic; uses only {@link System#nanoTime()} for measurement.
 */
public final class WorkloadCalibrator {

    /** Iterations used for the initial CPU pilot measurement. */
    private static final int CPU_PILOT_ITERATIONS = 50_000;

    /** Steps used for the initial MEMORY pilot measurement. */
    private static final int MEM_PILOT_STEPS = 10_000;

    /** Minimum unit count returned by calibration. */
    private static final int MIN_UNITS = 1;

    private WorkloadCalibrator() { }

    /* ================================================================
     *  CPU calibration
     * ================================================================ */

    /**
     * Estimates iterations so {@code new CpuBoundWorkload(seed, n).execute()}
     * takes ~{@code targetNanos} nanoseconds.
     */
    public static int calibrateIterations(long targetNanos, long seed) {
        if (targetNanos <= 0) {
            throw new IllegalArgumentException("targetNanos must be positive, got " + targetNanos);
        }

        long pilotNanos = timeCpu(seed, CPU_PILOT_ITERATIONS);
        if (pilotNanos <= 0) pilotNanos = 1;

        long estimated = (long) ((double) targetNanos / pilotNanos * CPU_PILOT_ITERATIONS);
        int firstGuess = clampInt(estimated);

        long verifyNanos = timeCpu(seed, firstGuess);
        if (verifyNanos <= 0) verifyNanos = 1;

        long corrected = (long) ((double) targetNanos / verifyNanos * firstGuess);
        return clampInt(corrected);
    }

    /* ================================================================
     *  MEMORY calibration
     * ================================================================ */

    /**
     * Estimates the step count so a {@link MemoryBoundWorkload} traversal
     * over {@code buffer} with {@code pattern} and {@code writeBack} takes
     * ~{@code targetNanos}.
     *
     * <p>Calibrated with the actual buffer + pattern + writeBack flag
     * that will be used at runtime, so cache footprint, access locality
     * and write-back traffic match the hot path. Calibrating with
     * {@code writeBack=false} when runtime uses {@code true} (or vice
     * versa) systematically mis-sizes MEMORY tasks because writes
     * touch the cache hierarchy differently from reads.
     */
    public static int calibrateMemorySteps(long targetNanos,
                                           long[] buffer,
                                           MemoryBoundWorkload.AccessPattern pattern,
                                           boolean writeBack,
                                           long seed) {
        if (targetNanos <= 0) {
            throw new IllegalArgumentException("targetNanos must be positive, got " + targetNanos);
        }

        long pilotNanos = timeMem(buffer, MEM_PILOT_STEPS, pattern, writeBack, seed);
        if (pilotNanos <= 0) pilotNanos = 1;

        long estimated = (long) ((double) targetNanos / pilotNanos * MEM_PILOT_STEPS);
        int firstGuess = clampInt(estimated);

        long verifyNanos = timeMem(buffer, firstGuess, pattern, writeBack, seed);
        if (verifyNanos <= 0) verifyNanos = 1;

        long corrected = (long) ((double) targetNanos / verifyNanos * firstGuess);
        return clampInt(corrected);
    }

    /* ================================================================
     *  Internal helpers
     * ================================================================ */

    private static long timeCpu(long seed, int iterations) {
        var w = new CpuBoundWorkload(seed, iterations);
        long start = System.nanoTime();
        long result = w.execute();
        long end = System.nanoTime();
        blackhole(result);
        return end - start;
    }

    private static long timeMem(long[] buffer,
                                int steps,
                                MemoryBoundWorkload.AccessPattern pattern,
                                boolean writeBack,
                                long seed) {
        var w = new MemoryBoundWorkload(buffer, steps, pattern, seed, writeBack);
        long start = System.nanoTime();
        long result = w.execute();
        long end = System.nanoTime();
        blackhole(result);
        return end - start;
    }

    @SuppressWarnings("unused")
    private static volatile long sink;

    private static void blackhole(long value) { sink = value; }

    private static int clampInt(long value) {
        if (value < MIN_UNITS) return MIN_UNITS;
        if (value > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) value;
    }
}
