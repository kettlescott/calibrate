package com.scott;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Independent monitor thread for CV/targetDeviation-based switching in HYBRID executor.
 *
 * <p>Runs at fixed 10ms intervals, collecting samples from all workers,
 * computing windowed CV or targetDeviation, and issuing switch requests when thresholds are crossed.
 *
 * <p>Never blocks workers: uses bounded buffers and volatile signaling.
 * Issues are logged with timestamps for debugging.
 *
 * <p>Switching metric:
 * <ul>
 *   <li>If config.targetMicros > 0: uses targetDeviation = sqrt(S/N) / T</li>
 *   <li>Otherwise: uses standard CV = stddev / mean</li>
 * </ul>
 */
public final class CVMonitorThread extends Thread {

    private static final long CHECK_INTERVAL_NANOS = 1_000_000L;   // 1ms (faster monitoring)
    private static final long WARMUP_DELAY_NANOS = 5_000_000_000L;  // 1s

    private final SampleBuffer[] sampleBuffers;
    private final SlidingWindowCV[] windowStats;
    private final AdaptiveHybridConfig config;
    private final long measurementStartNanos;
    private final AtomicReference<ExecutorState> currentState;
    private final AtomicBoolean stopRequested;
    private final CVSwitchListener listener;

    private long totalChecks;
    private long totalSwitches;
    private double lastMetricValue;    // Either CV or targetDeviation
    private long lastMean;
    private long lastSampleCount;
    private long nextDebugPrintNanos = -1L;
    private long lastMinNanos = 0L;
    private long lastMaxNanos = 0L;

    // Diagnostics for window age
    private long lastOldestNanos;
    private long lastNewestNanos;

    /**
     * Listener for switch requests.
     */
    public interface CVSwitchListener {
        /**
         * Called when a switch is requested.
         *
         * @param from current state
         * @param to target state
         * @param metricValue trigger value (CV or targetDeviation)
         * @param mean mean execution time (nanos)
         * @param sampleCount number of samples in window
         */
        void onSwitchRequested(ExecutorState from, ExecutorState to, double metricValue, long mean, long sampleCount);
    }

    public CVMonitorThread(SampleBuffer[] sampleBuffers,
                          AdaptiveHybridConfig config,
                          long measurementStartNanos,
                          AtomicReference<ExecutorState> currentState,
                          CVSwitchListener listener) {
        super("CVMonitorThread");
        this.sampleBuffers = sampleBuffers;
        this.config = config;
        this.measurementStartNanos = measurementStartNanos;
        this.currentState = currentState;
        this.listener = listener;
        this.stopRequested = new AtomicBoolean(false);

        // Create per-worker window statistics
        this.windowStats = new SlidingWindowCV[sampleBuffers.length];
        for (int i = 0; i < sampleBuffers.length; i++) {
            windowStats[i] = new SlidingWindowCV(config.windowMillis());
        }

        setDaemon(false);
    }

    @Override
    public void run() {
        try {
            // Wait for warmup to complete before starting CV checks
            while (!stopRequested.get()) {
                long now = System.nanoTime();
                long measurementElapsedNanos = now - measurementStartNanos;

                if (measurementElapsedNanos >= WARMUP_DELAY_NANOS) {
                    break;
                }

                long sleepNanos = Math.min(CHECK_INTERVAL_NANOS, WARMUP_DELAY_NANOS - measurementElapsedNanos);
                sleepNanos = Math.max(sleepNanos, 1_000_000L);  // At least 1ms
                sleepBriefly(sleepNanos);
            }

            for (SampleBuffer buffer : sampleBuffers) {
                //noinspection StatementWithEmptyBody
                while (buffer.pollSample() != null) {}
            }
            for (SlidingWindowCV window : windowStats) {
                window.reset();
            }
            System.out.println("[Hybrid] warmup flush");

            // Main monitoring loop
            double sharedEnterCv = config.sharedEnterCv();
            double shardEnterCv = config.shardedEnterCv();
            boolean closeHybrid = sharedEnterCv == 0 && shardEnterCv == 0;
            if (closeHybrid) {
                System.out.println("[Hybrid] Shutdown hybrid");
            }
            while (!stopRequested.get()) {
                long checkStartNanos = System.nanoTime();

                // Collect samples from all workers
                // This method now acquires its own timestamp after collection
                collectAndUpdateWindows();

                // Always compute CV - it's the only metric that controls switching
                double metricValue = computeAggregateCV();
                
                long mean = computeAggregateMean();
                long sampleCount = computeSampleCount();
                long[] minMax = computeAggregateMinMax();
                lastMinNanos = minMax[0];
                lastMaxNanos = minMax[1];

                lastMetricValue = metricValue;
                lastMean = mean;
                lastSampleCount = sampleCount;


                maybePrintCvDebug(checkStartNanos, metricValue, mean, sampleCount);
                // Check switching conditions
                if (!closeHybrid && !Double.isNaN(metricValue)) {
                    checkAndRequestSwitch(metricValue, mean, sampleCount);
                }

                totalChecks++;

                // Sleep until next check interval
                long checkElapsedNanos = System.nanoTime() - checkStartNanos;
                long sleepNanos = CHECK_INTERVAL_NANOS - checkElapsedNanos;
                if (sleepNanos > 0) {
                    sleepBriefly(sleepNanos);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sleepBriefly(long nanos) throws InterruptedException {
        long millis = nanos / 1_000_000L;
        int nanoRemainder = (int) (nanos % 1_000_000L);
        if (millis > 0) {
            Thread.sleep(millis, nanoRemainder);
        } else if (nanoRemainder > 0) {
            Thread.sleep(0, nanoRemainder);
        }
    }

    /**
     * Collects samples from all worker buffers and updates window statistics.
     * 
     * <p>Protocol (fixes time window basis):
     * 1. Collect a bounded, fair number of samples from each worker
     * 2. After collection completes, re-acquire current timestamp
     * 3. Use that timestamp to calculate window cutoff
     * 4. Expire all old samples using the same timestamp
     * 5. All statistics use the same snapshot time
     * 
     * <p>FIX-2: Condition order ensures we don't over-poll. Check limit FIRST,
     * then poll, so when we reach SAMPLES_PER_WORKER we stop and don't
     * discard an extra sample.
     */
    private void collectAndUpdateWindows() {
        // Phase 1: Fair, bounded sample collection from all workers
        // Collect at most N samples per worker (prevent one buffer from dominating)
        // Increased to handle higher throughput systems
        final int SAMPLES_PER_WORKER = 5000;
        
        long totalCollected = 0L;
        for (int workerIdx = 0; workerIdx < sampleBuffers.length; workerIdx++) {
            SampleBuffer buffer = sampleBuffers[workerIdx];
            SampleBuffer.Sample sample;
            int collected = 0;
            // FIX-2: Check limit FIRST to avoid over-polling
            while (collected < SAMPLES_PER_WORKER
                   && (sample = buffer.pollSample()) != null) {
                // Store sample temporarily; will process with proper timestamp
                windowStats[workerIdx].addSample(sample.completionNanos(), sample.executionNanos());
                collected++;
                totalCollected++;
            }
        }
        
        // Phase 2: Re-acquire current time AFTER collection is complete
        // This ensures the window calculation reflects the actual measurement moment
        long nowNanos = System.nanoTime();
        long windowNanos = config.windowMillis() * 1_000_000L;
        long cutoffNanos = nowNanos - windowNanos;
        
        // Phase 3: Expire old samples using the same snapshot time for all workers
        // This ensures all workers use identical window boundaries
        for (int workerIdx = 0; workerIdx < sampleBuffers.length; workerIdx++) {
            SlidingWindowCV window = windowStats[workerIdx];
            // Clean up any samples outside the window
            window.expireOldSamples(cutoffNanos);
        }
        
        // Phase 4: Optionally log dropped samples from buffers
        long totalBufferDropped = 0L;
        for (SampleBuffer buffer : sampleBuffers) {
            totalBufferDropped += buffer.droppedSampleCount();
        }
        
        // Warn if we collected nothing but still have drops (buffer overflow)
        if (totalCollected == 0 && totalBufferDropped > 0 && (totalChecks % 50 == 0)) {
//            System.out.printf("[Hybrid] WARNING: no samples collected in this cycle but %,d total samples dropped. "
//                    + "Buffer may be overflowing. Consider increasing CHECK_INTERVAL or SAMPLES_PER_WORKER.%n",
//                    totalBufferDropped);
        }
        if (totalBufferDropped > 0 && (totalChecks % 100 == 0)) {
//            System.out.printf("[Hybrid] warning: total buffer dropped samples=%,d, collected this cycle=%,d (check #%d)%n",
//                    totalBufferDropped, totalCollected, totalChecks);
        }
    }

    /**
     * Computes aggregate CV across all workers using Welford's algorithm.
     * 
     * <p>Formula:
     * <pre>
     * N = Σ n_i
     * mean = Σ(n_i × mean_i) / N
     * M2_total = Σ[M2_i + n_i × (mean_i - mean)²]
     * CV = sqrt(M2_total / N) / mean
     * </pre>
     *
     * <p>This correctly includes both within-worker variance (M2_i) and
     * between-worker variance, using the parallel algorithm for combining
     * independent Welford statistics.
     *
     * @return CV value, or NaN if insufficient data
     */
    private double computeAggregateCV() {
        // First pass: collect statistics and compute global mean
        long totalN = 0;
        double weightedMean = 0.0;

        for (SlidingWindowCV window : windowStats) {
            long n = window.getSampleCount();
            if (n == 0) {
                continue;
            }
            double mean_i = window.getMean();
            // Incrementally compute weighted mean
            weightedMean = (weightedMean * totalN + mean_i * n) / (totalN + n);
            totalN += n;
        }

        if (totalN < 2 || weightedMean <= 0.0) {
            return Double.NaN;
        }

        // Second pass: aggregate M2 contributions
        double totalM2 = 0.0;
        for (SlidingWindowCV window : windowStats) {
            long n = window.getSampleCount();
            if (n > 0) {
                double mean_i = window.getMean();
                double M2_i = window.getM2();
                // Within-worker variance
                totalM2 += M2_i;
                // Between-worker variance: n_i × (mean_i - global_mean)²
                double delta = mean_i - weightedMean;
                totalM2 += n * delta * delta;
            }
        }

        double variance = totalM2 / totalN;
        if (variance < 0.0) {
            variance = 0.0;  // Handle rounding
        }
        double stddev = Math.sqrt(variance);
        
        // CV = stddev / divisor
        double divisor = weightedMean;
        
        if (divisor <= 0.0) {
            return Double.NaN;
        }
        
        double cv = stddev / divisor;
        return Double.isFinite(cv) ? cv : Double.NaN;
    }

    /**
     * Computes aggregate targetDeviation across all workers.
     * 
     * <p>Formula: targetDeviation = sqrt(S / N) / T
     * <ul>
     *   <li>S = Σ_i Σ(x_ij - T)² (sum of squared differences from target)</li>
     *   <li>N = Σ n_i (total sample count)</li>
     *   <li>T = config.targetNanos (target value)</li>
     * </ul>
     *
     * <p>For each window, uses getTargetDeviationSumSquares() which efficiently
     * computes Σ(x - T)² using Welford's M2 relationship, avoiding full rescans.
     *
     * @return targetDeviation value, or NaN if not configured or insufficient data
     */
    private double computeAggregateTargetDeviation() {
        if (!config.isTargetDeviationMode()) {
            return Double.NaN;
        }

        long targetNanos = config.getTargetNanos();
        long totalN = 0;
        double totalSumSquaredDiffs = 0.0;

        // Aggregate across all workers
        for (SlidingWindowCV window : windowStats) {
            long n = window.getSampleCount();
            if (n > 0) {
                // Efficiently compute Σ(x_ij - T)² for this window
                double sumSquaredDiffs = window.getTargetDeviationSumSquares(targetNanos);
                totalSumSquaredDiffs += sumSquaredDiffs;
                totalN += n;
            }
        }

        if (totalN < 1) {
            return Double.NaN;
        }

        double variance = totalSumSquaredDiffs / totalN;
        double stddev = SlidingWindowCV.computeStddev(variance);
        double targetDeviation = stddev / targetNanos;
        return Double.isFinite(targetDeviation) ? targetDeviation : Double.NaN;
    }

    /**
     * Computes aggregate mean across all windows (for diagnostics).
     */
    private long computeAggregateMean() {
        long totalN = 0;
        double weightedSum = 0.0;

        for (SlidingWindowCV window : windowStats) {
            long n = window.getSampleCount();
            if (n == 0) {
                continue;
            }
            double mean = window.getMean();
            weightedSum += mean * n;
            totalN += n;
        }

        if (totalN == 0) {
            return 0L;
        }
        return Math.round(weightedSum / totalN);
    }

    /**
     * Computes total sample count across all windows.
     */
    private long computeSampleCount() {
        long total = 0;
        for (SlidingWindowCV window : windowStats) {
            total += window.getSampleCount();
        }
        return total;
    }

    /**
     * Computes aggregate min/max execution time across all windows.
     * Returns [minNanos, maxNanos].
     */
    private long[] computeAggregateMinMax() {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (SlidingWindowCV window : windowStats) {
            long count = window.getSampleCount();
            if (count == 0) {
                continue;
            }

            long[] rawStats = window.computeRawStatsNanos();
            long windowMin = rawStats[3];
            long windowMax = rawStats[2];

            if (windowMin < min) {
                min = windowMin;
            }
            if (windowMax > max) {
                max = windowMax;
            }
        }

        if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return new long[] { 0L, 0L };
        }

        return new long[] { min, max };
    }

    /**
     * Checks metric thresholds and requests switch if needed.
     */
    private void checkAndRequestSwitch(double metricValue, long mean, long sampleCount) {
        ExecutorState current = currentState.get();

        if (current.isTransitional()) {
            // Don't request new switches during transition
            return;
        }

        boolean shouldSwitch = false;
        ExecutorState targetState = null;

        if (current == ExecutorState.SHARDED && metricValue > config.sharedEnterCv()) {
            shouldSwitch = true;
            targetState = ExecutorState.SHARED;
        } else if (current == ExecutorState.SHARED && metricValue < config.shardedEnterCv()) {
            shouldSwitch = true;
            targetState = ExecutorState.SHARDED;
        }

        if (shouldSwitch) {
            listener.onSwitchRequested(current, targetState, metricValue, mean, sampleCount);
            totalSwitches++;
        }
    }

    private void maybePrintCvDebug(long currentNanos, double metricValue, long mean, long sampleCount) {
        long intervalSeconds = config.debugCvPrintIntervalSeconds();
        if (intervalSeconds <= 0) {
            return;
        }

        long intervalNanos = TimeUnit.SECONDS.toNanos(intervalSeconds);
        if (nextDebugPrintNanos < 0L) {
            nextDebugPrintNanos = currentNanos + intervalNanos;
        }

        // Only print if current time has reached the next scheduled print time
        if (currentNanos < nextDebugPrintNanos) {
            return;  // Not yet time to print; never backfill old snapshots
        }

        // Print exactly once with the current snapshot
        ExecutorState state = currentState.get();
        // Always show as 'cv' - targetMicros only affects the divisor calculation
        String metricLabel = "cv";
        
        // Compute window age diagnostics
        long oldestNanos = 0;
        long newestNanos = 0;
        long totalDropped = 0L;
        long totalBufferDropped = 0L;
        int totalCapacity = 0;
        for (int i = 0; i < windowStats.length; i++) {
            SlidingWindowCV window = windowStats[i];
            long oldest = window.getOldestCompletionNanos();
            long newest = window.getNewestCompletionNanos();
            if (oldest > 0 && (oldestNanos == 0 || oldest < oldestNanos)) {
                oldestNanos = oldest;
            }
            if (newest > newestNanos) {
                newestNanos = newest;
            }
            totalDropped += window.getDroppedDueToCapacityMax();
            totalBufferDropped += sampleBuffers[i].droppedSampleCount();
            totalCapacity += window.getCapacity();
        }
        
        long ageOldestMs = oldestNanos > 0 ? (currentNanos - oldestNanos) / 1_000_000L : 0L;
        long ageNewestMs = newestNanos > 0 ? (currentNanos - newestNanos) / 1_000_000L : 0L;
        this.lastOldestNanos = oldestNanos;
        this.lastNewestNanos = newestNanos;
        
        if (Double.isNaN(metricValue)) {
            // CV/targetDeviation is NaN (not enough samples)
            System.out.printf("[Hybrid] debug: state=%s, %s=%s (NaN), mean=%dus, samples=%d, "
                    + "windowMillis=%d, interval=%ds, ageMs(oldest=%d, newest=%d), "
                    + "capacity=%d, windowDropped=%d, bufferDropped=%d%n",
                    state.displayName(), metricLabel, "NaN", mean / 1000L, sampleCount, 
                    config.windowMillis(), intervalSeconds, ageOldestMs, ageNewestMs,
                    totalCapacity, totalDropped, totalBufferDropped);
        } else {
            // CV/targetDeviation is valid
            long[] minMax = computeAggregateMinMax();
            System.out.printf("[Hybrid] debug: state=%s, %s=%.4f, mean=%.1fus, samples=%d, "
                    + "windowMillis=%d, interval=%ds, ageMs(oldest=%d, newest=%d), "
                    + "capacity=%d, windowDropped=%d, bufferDropped=%d, min=%.1fus, max=%.1fus%n",
                    state.displayName(), metricLabel, metricValue, mean / 1000.0, sampleCount, 
                    config.windowMillis(), intervalSeconds, ageOldestMs, ageNewestMs,
                    totalCapacity, totalDropped, totalBufferDropped,
                    minMax[0] / 1000.0, minMax[1] / 1000.0);
        }
        
        // Print detailed window snapshot diagnostics
        printWindowSnapshot(currentNanos);
        
        // Schedule next print time
        nextDebugPrintNanos = currentNanos + intervalNanos;
    }

    /**
     * Prints detailed snapshot of each worker's window, including:
     * - Raw statistics (mean, stddev, max)
     * - Top 10 largest samples
     * - Validation of Welford's algorithm
     */
    private void printWindowSnapshot(long currentNanos) {
        for (int workerIdx = 0; workerIdx < windowStats.length; workerIdx++) {
            SlidingWindowCV window = windowStats[workerIdx];
            long sampleCount = window.getSampleCount();
            
            if (sampleCount == 0) {
                continue;
            }
            
            // Get raw statistics
            long[] rawStats = window.computeRawStatsNanos();
            long rawMeanNanos = rawStats[0];
            long rawStddevNanos = rawStats[1];
            long rawMaxNanos = rawStats[2];
            long rawMinNanos = rawStats[3];
            
            // Convert to micros for output
            long targetMicros = config.isTargetDeviationMode() ? config.targetMicros() : 0;
            long divisor = targetMicros > 0 ? targetMicros * 1_000L : rawMeanNanos; // divisor in nanos
            
            double meanMicros = rawMeanNanos / 1000.0;
            double stddevMicros = rawStddevNanos / 1000.0;
            double maxMicros = rawMaxNanos / 1000.0;
            double minMicros = rawMinNanos / 1000.0;
//            double cv = divisor > 0 ? (rawStddevNanos / (double) divisor) : Double.NaN;
            double cv = rawMeanNanos > 0.0
                ? 1d * rawStddevNanos / rawMeanNanos
                : Double.NaN;
            
//            System.out.printf("[Hybrid] Worker %d snapshot: samples=%d, "
//                    + "meanMicros=%.3f, stddevMicros=%.3f, maxMicros=%.3f, minMicros=%.3f, "
//                    + "divisor=%s, cv=%.4f%n",
//                    workerIdx, sampleCount,
//                    meanMicros, stddevMicros, maxMicros, minMicros,
//                    targetMicros > 0 ? (targetMicros + "µs") : "mean",
//                    cv);
            // Validate Welford's algorithm
            double[] discrepancies = window.validateWelfordStats();
            if (Math.abs(discrepancies[0]) > 1.0 || Math.abs(discrepancies[1]) > 1.0) {
                System.out.printf("[Hybrid] WARNING Worker %d: Welford discrepancy detected! "
                        + "mean_delta=%.1fns, stddev_delta=%.1fns%n",
                        workerIdx, discrepancies[0], discrepancies[1]);
            }
        }
    }

    /**
     * Stops the monitor thread gracefully.
     */
    public void stopMonitoring() {
        stopRequested.set(true);
    }

    /**
     * Returns total checks performed.
     */
    public long getTotalChecks() {
        return totalChecks;
    }

    /**
     * Returns total switches requested.
     */
    public long getTotalSwitches() {
        return totalSwitches;
    }

    /**
     * Returns last computed metric value (CV or targetDeviation).
     */
    public double getLastMetricValue() {
        return lastMetricValue;
    }

    /**
     * Returns last computed mean (in nanos).
     */
    public long getLastMean() {
        return lastMean;
    }

    /**
     * Returns last sample count.
     */
    public long getLastSampleCount() {
        return lastSampleCount;
    }

    /**
     * Diagnostic: returns whether targetDeviation mode is active.
     */
    public boolean isTargetDeviationMode() {
        return config.isTargetDeviationMode();
    }

    /**
     * Diagnostic: returns oldest sample completion timestamp in last check (nanos).
     */
    public long getLastOldestSampleNanos() {
        return lastOldestNanos;
    }

    /**
     * Diagnostic: returns newest sample completion timestamp in last check (nanos).
     */
    public long getLastNewestSampleNanos() {
        return lastNewestNanos;
    }

    /**
     * Diagnostic: returns latest aggregate minimum execution time in nanos.
     */
    public long getLastMinNanos() {
        return lastMinNanos;
    }

    /**
     * Diagnostic: returns latest aggregate maximum execution time in nanos.
     */
    public long getLastMaxNanos() {
        return lastMaxNanos;
    }
}
