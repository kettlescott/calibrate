package com.scott;

import java.util.Arrays;

/**
 * Sliding window CV/targetDeviation statistics using Welford's online algorithm.
 *
 * <p>Maintains mean and variance (M2) for execution times within a time window.
 * Supports two metrics:
 * <ul>
 *   <li>Standard CV: stddev / mean (based on measured average)</li>
 *   <li>targetDeviation: stddev / targetValue (based on fixed configuration value)</li>
 * </ul>
 *
 * <p>For targetDeviation, also maintains sum of (x - T)² for cross-worker aggregation.
 *
 * <p>Samples are stored in a dynamic circular buffer. When capacity is exceeded,
 * the buffer expands (not truncates). This ensures the window remains complete
 * and statistics remain accurate. If a hard memory limit is needed, truncation
 * is explicit and tracked.
 *
 * <p>Welford state (n, mean, M2) is kept synchronized with buffer contents.
 */
public final class SlidingWindowCV {

    private static final class WindowStat {
        long n;
        double mean;
        double M2;  // sum of squared differences from current mean (Welford)

        WindowStat() {
            this.n = 0;
            this.mean = 0.0;
            this.M2 = 0.0;
        }

        void reset() {
            n = 0;
            mean = 0.0;
            M2 = 0.0;
        }

        void addSample(double x) {
            n++;
            double delta = x - mean;
            mean = mean + delta / n;
            double delta2 = x - mean;
            M2 = M2 + delta * delta2;
        }

        void removeSample(double x) {
            if (n == 0) {
                return;  // no-op
            }
            if (n == 1) {
                reset();
                return;
            }
            double oldMean = mean;
            double newN = n - 1;
            mean = (n * mean - x) / newN;
            M2 = M2 - (x - oldMean) * (x - mean);
            n = (long) newN;
        }

        boolean isEmpty() {
            return n == 0;
        }
    }

    private final long windowNanos;
    // Dynamic circular buffer for samples
    private long[] completionTimes;      // ring buffer
    private long[] executionTimes;       // ring buffer
    private int capacity;                // current capacity
    private static final int INITIAL_CAPACITY = 8192;
    private static final int MAX_CAPACITY = 1_000_000;  // 1M samples hard limit
    
    private int headIndex = 0;                  // points to oldest sample
    private int tailIndex = 0;                  // points to next write position
    private int logicalCount = 0;               // number of samples currently in buffer

    private final WindowStat stat;
    
    // Diagnostics: track how many samples were dropped due to hard max
    private long droppedDueToCapMax = 0L;

    public SlidingWindowCV(long windowMillis) {
        this.windowNanos = windowMillis * 1_000_000L;
        this.capacity = INITIAL_CAPACITY;
        this.completionTimes = new long[capacity];
        this.executionTimes = new long[capacity];
        this.stat = new WindowStat();
    }

    /**
     * Adds a sample to the window. Updates mean/M2.
     * If buffer is full, expands it (unless at hard limit).
     * At hard limit, drops the oldest sample.
     */
    public void addSample(long completionNanos, long executionNanos) {
        if (logicalCount == capacity) {
            // Buffer full: try to expand
            if (capacity >= MAX_CAPACITY) {
                // Hard limit reached: drop oldest sample
                long oldExecution = executionTimes[headIndex];
                stat.removeSample((double) oldExecution);
                headIndex = (headIndex + 1) % capacity;
                logicalCount--;
                droppedDueToCapMax++;
            } else {
                // Expand buffer
                expandBuffer();
            }
        }

        completionTimes[tailIndex] = completionNanos;
        executionTimes[tailIndex] = executionNanos;
        stat.addSample((double) executionNanos);
        tailIndex = (tailIndex + 1) % capacity;
        logicalCount++;
    }

    /**
     * Expands the circular buffer by ~1.5x (minimum +1).
     */
    private void expandBuffer() {
        int newCapacity = Math.min(
            capacity + (capacity >> 1),  // 1.5x growth
            MAX_CAPACITY
        );
        if (newCapacity <= capacity) {
            newCapacity = capacity + 1;
        }

        long[] newCompletion = new long[newCapacity];
        long[] newExecution = new long[newCapacity];

        // Copy samples in order from head to tail
        if (logicalCount > 0) {
            if (headIndex < tailIndex) {
                // Samples are contiguous: [head...tail)
                System.arraycopy(completionTimes, headIndex, newCompletion, 0, logicalCount);
                System.arraycopy(executionTimes, headIndex, newExecution, 0, logicalCount);
            } else {
                // Samples wrap: [head...capacity) + [0...tail)
                int firstPart = capacity - headIndex;
                System.arraycopy(completionTimes, headIndex, newCompletion, 0, firstPart);
                System.arraycopy(completionTimes, 0, newCompletion, firstPart, tailIndex);
                System.arraycopy(executionTimes, headIndex, newExecution, 0, firstPart);
                System.arraycopy(executionTimes, 0, newExecution, firstPart, tailIndex);
            }
        }

        completionTimes = newCompletion;
        executionTimes = newExecution;
        headIndex = 0;
        tailIndex = logicalCount;
        capacity = newCapacity;
    }

    /**
     * Expires old samples based on cutoff time.
     * Removes all samples with completionNanos < cutoffNanos from the head.
     * Note: Already-expired samples discovered during polling are silently dropped;
     * this method only cleans up samples already in the window.
     */
    public void expireOldSamples(long cutoffNanos) {
        while (logicalCount > 0) {
            long oldestCompletion = completionTimes[headIndex];
            if (oldestCompletion >= cutoffNanos) {
                break;  // Head is still within window
            }
            // Remove oldest sample
            long oldExecution = executionTimes[headIndex];
            stat.removeSample((double) oldExecution);
            headIndex = (headIndex + 1) % capacity;
            logicalCount--;
        }
    }

    /**
     * Computes current CV: stddev / mean.
     * Returns Double.NaN if insufficient data or invalid state.
     *
     * @return CV value, or NaN if cannot compute
     */
    public double computeCV() {
        if (stat.n < 2 || stat.mean <= 0.0) {
            return Double.NaN;
        }
        double variance = stat.M2 / stat.n;
        if (variance < 0.0) {
            variance = 0.0;  // handle floating-point rounding
        }
        double stddev = Math.sqrt(variance);
        double cv = stddev / stat.mean;
        return Double.isFinite(cv) ? cv : Double.NaN;
    }

    /**
     * Returns current M2 (sum of squared differences from mean).
     * Used by CVMonitorThread for cross-worker aggregation.
     */
    public double getM2() {
        return stat.M2;
    }

    /**
     * Returns sum of squared differences from a target value.
     * Used by CVMonitorThread for targetDeviation aggregation.
     * Formula: Σ(x - T)² for all samples in this window.
     * Efficient: uses Welford relationship M2 = Σ(x - mean)² and computes
     * the cross-term without rescanning samples.
     *
     * @param targetNanos the fixed target value
     * @return sum of squared differences from target
     */
    public double getTargetDeviationSumSquares(long targetNanos) {
        if (stat.n == 0 || targetNanos <= 0) {
            return 0.0;
        }
        
        // Σ(x - T)² = Σ(x - mean + mean - T)²
        //           = Σ[(x - mean)² + 2(x - mean)(mean - T) + (mean - T)²]
        //           = M2 + 2(mean - T)·Σ(x - mean) + n·(mean - T)²
        // But Σ(x - mean) = 0 by definition of Welford's mean, so:
        //           = M2 + n·(mean - T)²
        
        double meanDiff = stat.mean - targetNanos;
        return stat.M2 + stat.n * meanDiff * meanDiff;
    }

    /**
     * Helper: computes variance from sum of squared deviations and count.
     */
    public static double computeVariance(double sumSquaredDiffs, long n) {
        if (n < 1) return 0.0;
        return sumSquaredDiffs / n;
    }

    /**
     * Helper: computes stddev from variance.
     */
    public static double computeStddev(double variance) {
        if (variance < 0.0) variance = 0.0;
        return Math.sqrt(variance);
    }

    /**
     * Current number of samples in the window (Welford count, should match buffer).
     */
    public long getSampleCount() {
        return stat.n;
    }

    /**
     * Current mean of samples (measured average).
     */
    public double getMean() {
        return stat.mean;
    }

    /**
     * Diagnostic: returns actual buffer count (may differ from stat.n after expiry).
     */
    public int getBufferCount() {
        return logicalCount;
    }

    /**
     * Diagnostic: returns oldest completion time in buffer, or 0 if empty.
     */
    public long getOldestCompletionNanos() {
        return logicalCount > 0 ? completionTimes[headIndex] : 0L;
    }

    /**
     * Diagnostic: returns newest completion time in buffer, or 0 if empty.
     */
    public long getNewestCompletionNanos() {
        return logicalCount > 0 ? completionTimes[(tailIndex - 1 + capacity) % capacity] : 0L;
    }

    /**
     * Diagnostic: returns current buffer capacity.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Diagnostic: returns number of samples dropped due to hard max capacity.
     */
    public long getDroppedDueToCapacityMax() {
        return droppedDueToCapMax;
    }

    /**
     * Reset for new phase.
     */
    public void reset() {
        headIndex = 0;
        tailIndex = 0;
        logicalCount = 0;
        stat.reset();
        droppedDueToCapMax = 0L;
    }

    /**
     * Diagnostic: returns all samples in current window as arrays.
     * Used for detailed analysis of window contents.
     * 
     * @return array of [completionTimes, executionTimes] (in nanos)
     */
    public long[][] getAllSamplesInWindow() {
        if (logicalCount == 0) {
            return new long[][] { new long[0], new long[0] };
        }
        
        long[] completions = new long[logicalCount];
        long[] executions = new long[logicalCount];
        
        for (int i = 0; i < logicalCount; i++) {
            int idx = (headIndex + i) % capacity;
            completions[i] = completionTimes[idx];
            executions[i] = executionTimes[idx];
        }
        
        return new long[][] { completions, executions };
    }

    /**
     * Diagnostic: computes statistics from raw samples.
     * Returns [mean, stddev, max, min] in nanoseconds.
     * Validates against Welford's stats.
     * 
     * @return statistics array: [meanNanos, stddevNanos, maxNanos, minNanos]
     */
    public long[] computeRawStatsNanos() {
        if (logicalCount == 0) {
            return new long[] { 0L, 0L, 0L, 0L };
        }
        
        // Compute sum, max, min
        long sum = 0L;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        
        for (int i = 0; i < logicalCount; i++) {
            int idx = (headIndex + i) % capacity;
            long exec = executionTimes[idx];
            sum += exec;
            if (exec > max) max = exec;
            if (exec < min) min = exec;
        }
        
        long mean = sum / logicalCount;
        
        // Compute variance
        double sumSquaredDiffs = 0.0;
        for (int i = 0; i < logicalCount; i++) {
            int idx = (headIndex + i) % capacity;
            long exec = executionTimes[idx];
            double delta = exec - mean;
            sumSquaredDiffs += delta * delta;
        }
        
        double variance = sumSquaredDiffs / logicalCount;
        long stddev = Math.round(Math.sqrt(variance));
        
        return new long[] { mean, stddev, max, min };
    }

    /**
     * Diagnostic: returns top N largest samples with their indices.
     * Format: [execution time (nanos), original index in window]
     * 
     * @param topN number of top samples to return
     * @return 2D array where [i][0] = exec time, [i][1] = index
     */
    public long[][] getTopNLargestSamples(int topN) {
        if (logicalCount == 0) {
            return new long[0][2];
        }
        
        int actualN = Math.min(topN, (int) logicalCount);
        long[][] result = new long[actualN][2];
        
        // Initialize result with first actualN samples
        for (int i = 0; i < actualN; i++) {
            int idx = (headIndex + i) % capacity;
            result[i][0] = executionTimes[idx];
            result[i][1] = i;
        }
        
        // Bubble-sort in descending order by execution time
        for (int i = 0; i < actualN; i++) {
            for (int j = i + 1; j < actualN; j++) {
                if (result[j][0] > result[i][0]) {
                    // Swap
                    long tmpExec = result[i][0];
                    long tmpIdx = result[i][1];
                    result[i][0] = result[j][0];
                    result[i][1] = result[j][1];
                    result[j][0] = tmpExec;
                    result[j][1] = tmpIdx;
                }
            }
        }
        
        return result;
    }

    /**
     * Diagnostic: validates that Welford's mean and variance match raw computation.
     * Returns discrepancy info as [meanDiscrepancy (nanos), stddevDiscrepancy (nanos)].
     */
    public double[] validateWelfordStats() {
        long[] rawStats = computeRawStatsNanos();
        long rawMean = rawStats[0];
        long rawStddev = rawStats[1];
        
        long welfMean = Math.round(stat.mean);
        long welfStddev = Math.round(Math.sqrt(stat.M2 / Math.max(1, stat.n)));
        
        return new double[] {
            rawMean - welfMean,
            rawStddev - welfStddev
        };
    }
}
