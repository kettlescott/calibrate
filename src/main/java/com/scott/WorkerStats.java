package com.scott;

/**
 * Per-worker statistics collected by {@link ShardedWorker} when
 * {@link DiagnosticsConfig#enabled()} is true.
 *
 * <h3>Threading</h3>
 * Each instance is owned by exactly one worker thread. All fields are
 * mutated only from that worker's {@code run()} method, and read only
 * after the worker has terminated (post-join happens-before). No
 * volatiles, no atomics, no locks — the recording cost is a handful of
 * primitive long ops per task.
 *
 * <h3>Optional histogram</h3>
 * When {@link DiagnosticsConfig#perWorkerLatency()} is true, a
 * {@link LatencyRecorder} is attached and fed via {@code record(task)}.
 * This adds four long stores per task to the per-worker recorder
 * (allocation-free), because {@link LatencyRecorder} keeps four metrics
 * — submit overhead, queue wait, execution and end-to-end — each in its
 * own {@code long[]} buffer. Approximate raw cost:
 * {@code tasksPerWorker × 4 × 8 bytes} plus {@code LongBuffer}
 * resize / overhead. For a 10 M-task / 32-worker run this is
 * {@code ~10 MB per worker} — ~320 MB across all workers.
 *
 * <p><b>Heavy — intended for diagnostic runs with bounded
 * {@code taskCount} only.</b> Do not enable for headline throughput /
 * latency experiments; the GC pressure and resident memory will distort
 * the very tail you are trying to measure.
 *
 * <p>When the flag is false, only the cheap counters below are
 * maintained.
 */
final class WorkerStats {

    /* Always-on cheap counters (single-writer; no sync).
     * Measurement-only — warmup tasks are filtered out at the top of
     * onTaskCompleted(). */
    long processed;
    long execSumNs;
    long execMaxNs = Long.MIN_VALUE;
    long queueWaitSumNs;
    long queueWaitMaxNs = Long.MIN_VALUE;

    /**
     * Sampling-only view of {@link #processed}, published every 1024
     * measurement tasks and on {@link #publishFinal()}. The window
     * sampler reads this (volatile) instead of {@code processed} (plain
     * long) so it cannot observe torn/stale values across CPUs. The
     * volatile store is amortised at one write per ~1024 tasks ≪ 1 ns
     * per task — well below the cost of {@code task.run()}.
     */
    volatile long publishedProcessed;

    /* Slow-burst tracking — driven by a static threshold from YAML.
     * No p95-based dynamic threshold (forbidden by the spec). */
    final long slowThresholdNs;
    long slowTotal;
    long slowStreakCurrent;
    long slowStreakMax;

    /* Optional per-worker histogram (full percentiles). */
    final LatencyRecorder histogram;   // null when perWorkerLatency=false

    /* Optional per-worker inter-task gap diagnostics. */
    final InterTaskGapDiagnostics gapDiags;  // null when diagnostics disabled

    WorkerStats(long slowThresholdNs, boolean perWorkerLatency, int expectedTasksHint) {
        this.slowThresholdNs = slowThresholdNs;
        this.histogram = perWorkerLatency
                ? new LatencyRecorder(Math.max(1024, expectedTasksHint))
                : null;
        // Allocate gap diagnostics if perWorkerLatency is enabled (uses same histogram allocation hint)
        this.gapDiags = perWorkerLatency
                ? new InterTaskGapDiagnostics(Math.max(1024, expectedTasksHint))
                : null;
    }

    /**
     * Called by the worker thread immediately after {@code task.run()}
     * returns. Single-writer, allocation-free, no syncing.
     *
     * <p>Warmup tasks are filtered out — only measurement tasks
     * contribute to any diagnostic counter, mirroring the main
     * latency-recorder behaviour so the diagnostics block in
     * {@code summary_sharded50.txt} describes the measurement window only.
     */
    void onTaskCompleted(Task task) {
        if (!task.isMeasurement()) {
            return;
        }
        long ex  = task.executionTimeNanos();
        long qw  = task.queueWaitTimeNanos();

        processed++;
        execSumNs += ex;
        if (ex > execMaxNs) execMaxNs = ex;
        queueWaitSumNs += qw;
        if (qw > queueWaitMaxNs) queueWaitMaxNs = qw;

        if (slowThresholdNs > 0L && ex >= slowThresholdNs) {
            slowTotal++;
            slowStreakCurrent++;
            if (slowStreakCurrent > slowStreakMax) slowStreakMax = slowStreakCurrent;
        } else {
            slowStreakCurrent = 0L;
        }

        if (histogram != null) {
            histogram.record(task);
        }

        // Amortised publication for the window sampler. One volatile
        // store every 1024 tasks — negligible vs. task.run() cost, and
        // small enough that the sampler sees fresh values within one
        // ~1024-task batch (well under the 1 s window default).
        if ((processed & 1023L) == 0L) {
            publishedProcessed = processed;
        }
    }

    /**
     * Records inter-task gap metrics when gap diagnostics are enabled.
     * Called by the worker thread between dequeue and task execution.
     *
     * @param gapNs         time from previousTaskFinish to currentTaskStart
     * @param pollNs        time spent dequeuing
     * @param emptyQueueNs  time waiting when queue was empty
     * @param nonEmptyQueueNs time spent dequeuing when queue was non-empty
     * @param executionNs   task execution time
     */
    void recordInterTaskGap(long gapNs, long pollNs, long emptyQueueNs, long nonEmptyQueueNs, long executionNs) {
        if (gapDiags != null) {
            gapDiags.recordInterTaskGap(gapNs, pollNs, emptyQueueNs, nonEmptyQueueNs, executionNs);
        }
    }

    /**
     * Publishes the final processed count so the post-drain summary
     * pass and the final window snapshot see it. Called once after
     * the worker has finished all measurement tasks.
     */
    void publishFinal() {
        publishedProcessed = processed;
    }

    /** Snapshot processed count for window sampling (volatile read). */
    long processedCount() { return publishedProcessed; }

    /** Access gap diagnostics (for post-run reporting). */
    InterTaskGapDiagnostics gapDiagnostics() { return gapDiags; }
}

