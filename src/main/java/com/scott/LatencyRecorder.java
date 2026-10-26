package com.scott;

import java.util.Arrays;

/**
 * Collects latency data from completed {@link Task} objects and computes
 * percentile + max statistics (p50, p90, p95, p99, max) for:
 * <ul>
 *   <li>submit/build overhead  (enqueuedNanos - createdNanos)</li>
 *   <li>queue wait             (startNanos    - enqueuedNanos)</li>
 *   <li>execution              (finishNanos   - startNanos)</li>
 *   <li>end-to-end             (finishNanos   - createdNanos)</li>
 * </ul>
 *
 * <p>Samples are stored in primitive {@code long} buffers
 * ({@link LongBuffer}) to avoid {@link Long} boxing and minimise
 * young-gen GC pressure during measurement. Max values are tracked
 * incrementally on {@link #record(Task)} — no second pass, no boxing.
 *
 * <p>Percentile computation runs <em>after</em> all tasks have completed
 * (offline analysis). Call {@link #record(Task)} for each completed
 * task, then {@link #summary()} for a formatted report.
 */
public final class LatencyRecorder {

    private final LongBuffer submitOverheadNanos;
    private final LongBuffer queueWaitNanos;
    private final LongBuffer executionNanos;
    private final LongBuffer endToEndNanos;

    // Incremental max trackers -- updated per record(), O(1) per sample, no boxing.
    private long maxSubmitOverheadNanos = Long.MIN_VALUE;
    private long maxQueueWaitNanos      = Long.MIN_VALUE;
    private long maxExecutionNanos      = Long.MIN_VALUE;
    private long maxEndToEndNanos       = Long.MIN_VALUE;

    /**
     * Creates a recorder with a default initial capacity (1024).
     * Suitable for ad-hoc runs; for formal benchmarks prefer
     * {@link #LatencyRecorder(int)} with the exact task count.
     */
    public LatencyRecorder() {
        this(1024);
    }

    /**
     * Creates a recorder pre-sized for {@code expectedTasks} samples.
     * When the exact task count is known up front this avoids any
     * runtime buffer growth, eliminating allocation on the recording path.
     *
     * @param expectedTasks anticipated number of tasks to record
     */
    public LatencyRecorder(int expectedTasks) {
        this.submitOverheadNanos = new LongBuffer(expectedTasks);
        this.queueWaitNanos      = new LongBuffer(expectedTasks);
        this.executionNanos      = new LongBuffer(expectedTasks);
        this.endToEndNanos       = new LongBuffer(expectedTasks);
    }

    /* ================================================================
     *  Recording
     * ================================================================ */

    /**
     * Records the latency data from a single completed task. Only
     * primitive {@code long} values are stored; no boxing occurs.
     */
    public void record(Task task) {
        long so = task.submitOverheadNanos();
        long qw = task.queueWaitTimeNanos();
        long ex = task.executionTimeNanos();
        long e2e = task.endToEndLatencyNanos();

        recordRaw(so, qw, ex, e2e);
    }

    /**
     * Records primitive latency samples directly.
     *
     * <p>Used by the non-retaining measurement path that aggregates
     * latencies online from completion callbacks without storing full
     * {@link Task} objects.</p>
     */
    public void recordRaw(long so, long qw, long ex, long e2e) {

        submitOverheadNanos.add(so);
        queueWaitNanos.add(qw);
        executionNanos.add(ex);
        endToEndNanos.add(e2e);

        if (so  > maxSubmitOverheadNanos) maxSubmitOverheadNanos = so;
        if (qw  > maxQueueWaitNanos)      maxQueueWaitNanos      = qw;
        if (ex  > maxExecutionNanos)      maxExecutionNanos      = ex;
        if (e2e > maxEndToEndNanos)       maxEndToEndNanos       = e2e;
    }

    /**
     * Records latency data from every task in the list.
     */
    public void recordAll(Task[] tasks) {
        for (Task t : tasks) {
            record(t);
        }
    }

    /** Returns the number of tasks recorded so far. */
    public int recordedTasks() {
        return queueWaitNanos.size();
    }

    /* ================================================================
     *  Percentiles
     * ================================================================ */

    public double p50SubmitOverhead() { return percentile(submitOverheadNanos, 50); }
    public double p90SubmitOverhead() { return percentile(submitOverheadNanos, 90); }
    public double p95SubmitOverhead() { return percentile(submitOverheadNanos, 95); }
    public double p99SubmitOverhead() { return percentile(submitOverheadNanos, 99); }

    public double p50QueueWait()  { return percentile(queueWaitNanos, 50); }
    public double p90QueueWait()  { return percentile(queueWaitNanos, 90); }
    public double p95QueueWait()  { return percentile(queueWaitNanos, 95); }
    public double p99QueueWait()  { return percentile(queueWaitNanos, 99); }

    public double p50Execution()  { return percentile(executionNanos, 50); }
    public double p90Execution()  { return percentile(executionNanos, 90); }
    public double p95Execution()  { return percentile(executionNanos, 95); }
    public double p99Execution()  { return percentile(executionNanos, 99); }

    public double p50EndToEnd()   { return percentile(endToEndNanos, 50); }
    public double p90EndToEnd()   { return percentile(endToEndNanos, 90); }
    public double p95EndToEnd()   { return percentile(endToEndNanos, 95); }
    public double p99EndToEnd()   { return percentile(endToEndNanos, 99); }

    /* ================================================================
     *  Max values (nanoseconds)
     * ================================================================ */

    public long maxSubmitOverhead() { return submitOverheadNanos.isEmpty() ? 0L : maxSubmitOverheadNanos; }
    public long maxQueueWait()      { return queueWaitNanos.isEmpty()      ? 0L : maxQueueWaitNanos;      }
    public long maxExecution()      { return executionNanos.isEmpty()      ? 0L : maxExecutionNanos;      }
    public long maxEndToEnd()       { return endToEndNanos.isEmpty()       ? 0L : maxEndToEndNanos;       }

    /* ================================================================
     *  Averages (nanoseconds)
     *  Computed once at end-of-run for summary reporting; the recording
     *  hot path stays allocation-free.
     * ================================================================ */

    public double avgExecution()      { return mean(executionNanos); }
    public double avgQueueWait()      { return mean(queueWaitNanos); }
    public double avgEndToEnd()       { return mean(endToEndNanos); }
    public double avgSubmitOverhead() { return mean(submitOverheadNanos); }

    /* ================================================================
     *  Summary
     * ================================================================ */

    /**
     * Formatted multi-line summary with p50 / p90 / p95 / p99 / max in
     * milliseconds, for all four metrics.
     */
    public String summary() {
        if (queueWaitNanos.isEmpty()) {
            return "LatencyRecorder: no data recorded.";
        }

        var sb = new StringBuilder();
        sb.append(String.format("Recorded tasks: %d%n%n", queueWaitNanos.size()));

        sb.append(String.format("%-16s %10s %10s %10s %10s %10s%n",
                "Metric", "p50", "p90", "p95", "p99", "max"));
        sb.append("-".repeat(72)).append('\n');

        sb.append(formatRow("Submit overhead",
                p50SubmitOverhead(), p90SubmitOverhead(), p95SubmitOverhead(), p99SubmitOverhead(),
                maxSubmitOverhead()));
        sb.append(formatRow("Queue wait",
                p50QueueWait(), p90QueueWait(), p95QueueWait(), p99QueueWait(),
                maxQueueWait()));
        sb.append(formatRow("Execution",
                p50Execution(), p90Execution(), p95Execution(), p99Execution(),
                maxExecution()));
        sb.append(formatRow("End-to-end",
                p50EndToEnd(), p90EndToEnd(), p95EndToEnd(), p99EndToEnd(),
                maxEndToEnd()));

        return sb.toString();
    }

    /* ================================================================
     *  Internals
     * ================================================================ */

    private static double percentile(LongBuffer data, int percentile) {
        if (data.isEmpty()) return 0.0;

        long[] sorted = data.toArray();
        Arrays.sort(sorted);

        // nearest-rank: index = ceil(percentile / 100 * N) - 1, clamped
        int index = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index];
    }

    private static double mean(LongBuffer data) {
        if (data.isEmpty()) return 0.0;
        long[] arr = data.toArray();
        long sum = 0L;
        for (long v : arr) sum += v;
        return (double) sum / arr.length;
    }


    private static String formatRow(String label,
                                    double p50, double p90, double p95, double p99,
                                    long maxNanos) {
        return String.format("%-16s %9.3f ms %9.3f ms %9.3f ms %9.3f ms %9.3f ms%n",
                label,
                p50 / 1_000_000.0,
                p90 / 1_000_000.0,
                p95 / 1_000_000.0,
                p99 / 1_000_000.0,
                maxNanos / 1_000_000.0);
    }

    /**
     * Calculates throughput given a duration in seconds.
     * @param durationSeconds elapsed time during measurement
     * @return tasks per second
     */
    public double throughputTasksPerSecond(double durationSeconds) {
        if (durationSeconds <= 0.0) {
            return 0.0;
        }
        return recordedTasks() / durationSeconds;
    }
}

