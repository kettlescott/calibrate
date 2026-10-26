package com.scott;

import java.util.Arrays;

/**
 * Per-worker diagnostics for inter-task gap analysis.
 *
 * <p>Tracks the time between finishing one task and starting the next, distinguishing:
 * <ul>
 *   <li>Poll/dequeue latency: time spent acquiring the next task from the queue</li>
 *   <li>Empty queue time: time spent waiting when the queue was empty</li>
 *   <li>Non-empty queue time: time spent dequeuing from a populated queue</li>
 * </ul>
 *
 * <p>Instrumented with minimal overhead:
 * <ul>
 *   <li>No per-task allocation or logging</li>
 *   <li>Aggregate statistics only (sum, count, percentiles)</li>
 *   <li>Optional histogram collection via circular buffer</li>
 * </ul>
 *
 * <p><b>Threading:</b> Single-writer (owned worker thread) until stats are finalized
 * after the worker exits.
 */
final class InterTaskGapDiagnostics {

    // ---- Cheap counters: always collected ----
    long tasksProcessed;
    long executionTimeSumNs;           // task execution time only
    long interTaskGapTimeSumNs;         // time between finishing one task and starting next
    long pollTimeSumNs;                 // time from starting dequeue until task obtained
    long emptyQueueTimeSumNs;           // subset of pollTime spent when queue was empty
    long nonEmptyQueueTimeSumNs;        // subset of pollTime spent with queue non-empty

    long interTaskGapMaxNs = Long.MIN_VALUE;
    long pollTimeMaxNs = Long.MIN_VALUE;
    long emptyQueueTimeMaxNs = Long.MIN_VALUE;
    long nonEmptyQueueTimeMaxNs = Long.MIN_VALUE;

    // ---- Histogram (optional, circular buffer) ----
    // Only allocated when perWorkerLatency is enabled to avoid memory waste.
    // Records raw timestamps; percentiles computed post-run.
    private final long[] interTaskGapHistogram;
    private final long[] pollTimeHistogram;
    private int histogramIndex;

    /**
     * Creates diagnostics with optional histogram collection.
     *
     * @param histogramSize if > 0, allocate histograms to record raw values;
     *                      if ≤ 0, collect only aggregate statistics
     */
    InterTaskGapDiagnostics(int histogramSize) {
        if (histogramSize > 0) {
            this.interTaskGapHistogram = new long[histogramSize];
            this.pollTimeHistogram = new long[histogramSize];
        } else {
            this.interTaskGapHistogram = null;
            this.pollTimeHistogram = null;
        }
        this.histogramIndex = 0;
    }

    /**
     * Records an inter-task gap observation.
     *
     * @param gapNs         time from previousTaskFinish to currentTaskStart
     * @param pollNs        time spent dequeuing (may be subset of gapNs)
     * @param emptyQueueNs  time spent waiting when queue was empty
     * @param nonEmptyQueueNs time spent dequeuing when queue was non-empty
     */
    void recordInterTaskGap(long gapNs, long pollNs, long emptyQueueNs, long nonEmptyQueueNs, long executionNs) {
        tasksProcessed++;
        executionTimeSumNs += executionNs;
        interTaskGapTimeSumNs += gapNs;
        pollTimeSumNs += pollNs;
        emptyQueueTimeSumNs += emptyQueueNs;
        nonEmptyQueueTimeSumNs += nonEmptyQueueNs;

        if (gapNs > interTaskGapMaxNs) interTaskGapMaxNs = gapNs;
        if (pollNs > pollTimeMaxNs) pollTimeMaxNs = pollNs;
        if (emptyQueueNs > emptyQueueTimeMaxNs) emptyQueueTimeMaxNs = emptyQueueNs;
        if (nonEmptyQueueNs > nonEmptyQueueTimeMaxNs) nonEmptyQueueTimeMaxNs = nonEmptyQueueNs;

        if (interTaskGapHistogram != null) {
            int idx = histogramIndex % interTaskGapHistogram.length;
            interTaskGapHistogram[idx] = gapNs;
            pollTimeHistogram[idx] = pollNs;
            histogramIndex++;
        }
    }

    /**
     * Compute percentiles from collected histogram.
     * @param percentile value in [0, 100]
     * @return percentile value, or -1 if histogram not collected
     */
    long interTaskGapPercentile(double percentile) {
        if (interTaskGapHistogram == null || tasksProcessed == 0) return -1;
        int count = (int) Math.min(tasksProcessed, interTaskGapHistogram.length);
        long[] sorted = new long[count];
        System.arraycopy(interTaskGapHistogram, 0, sorted, 0, count);
        Arrays.sort(sorted);
        int idx = (int) Math.ceil((percentile / 100.0) * count) - 1;
        return sorted[Math.max(0, Math.min(idx, count - 1))];
    }

    long pollTimePercentile(double percentile) {
        if (pollTimeHistogram == null || tasksProcessed == 0) return -1;
        int count = (int) Math.min(tasksProcessed, pollTimeHistogram.length);
        long[] sorted = new long[count];
        System.arraycopy(pollTimeHistogram, 0, sorted, 0, count);
        Arrays.sort(sorted);
        int idx = (int) Math.ceil((percentile / 100.0) * count) - 1;
        return sorted[Math.max(0, Math.min(idx, count - 1))];
    }

    // ---- Public accessors for reporters ----

    /**
     * Checks if histogram data is available for percentile queries.
     * @return true if histograms were allocated and collected
     */
    public boolean hasHistograms() {
        return interTaskGapHistogram != null && pollTimeHistogram != null;
    }

    // ---- Accessors (all measurement-phase only) ----

    long tasksProcessed() { return tasksProcessed; }
    long executionTimeAvgNs() { return tasksProcessed > 0 ? executionTimeSumNs / tasksProcessed : 0; }
    long interTaskGapAvgNs() { return tasksProcessed > 0 ? interTaskGapTimeSumNs / tasksProcessed : 0; }
    long pollTimeAvgNs() { return tasksProcessed > 0 ? pollTimeSumNs / tasksProcessed : 0; }
    long emptyQueueTimeAvgNs() { return tasksProcessed > 0 ? emptyQueueTimeSumNs / tasksProcessed : 0; }
    long nonEmptyQueueTimeAvgNs() { return tasksProcessed > 0 ? nonEmptyQueueTimeSumNs / tasksProcessed : 0; }

    long interTaskGapMaxNs() { return interTaskGapMaxNs == Long.MIN_VALUE ? 0 : interTaskGapMaxNs; }
    long pollTimeMaxNs() { return pollTimeMaxNs == Long.MIN_VALUE ? 0 : pollTimeMaxNs; }
    long emptyQueueTimeMaxNs() { return emptyQueueTimeMaxNs == Long.MIN_VALUE ? 0 : emptyQueueTimeMaxNs; }
    long nonEmptyQueueTimeMaxNs() { return nonEmptyQueueTimeMaxNs == Long.MIN_VALUE ? 0 : nonEmptyQueueTimeMaxNs; }

    double emptyQueueRatio() {
        long total = emptyQueueTimeSumNs + nonEmptyQueueTimeSumNs;
        return total > 0 ? (double) emptyQueueTimeSumNs / total : 0.0;
    }
}

