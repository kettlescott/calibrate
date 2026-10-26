package com.scott;

import java.util.List;

/**
 * Reporter for inter-task gap diagnostics.
 *
 * <p>Collects and formats per-worker gap statistics including:
 * <ul>
 *   <li>Tasks processed and execution time</li>
 *   <li>Inter-task gap: avg/p50/p95/p99/max</li>
 *   <li>Poll/dequeue latency: avg/p50/p95/p99/max</li>
 *   <li>Queue state: empty vs non-empty time breakdown</li>
 * </ul>
 */
public final class InterTaskGapReporter {

    private final List<WorkerStats> perWorkerStats;
    private final List<QueueStatistics> perQueueStats;

    public InterTaskGapReporter(List<WorkerStats> perWorkerStats, List<QueueStatistics> perQueueStats) {
        this.perWorkerStats = perWorkerStats;
        this.perQueueStats = perQueueStats;
    }

    /**
     * Generates comprehensive gap diagnostics for all workers and queues as a string.
     * Can be printed to console or written to file.
     */
    public String generateDiagnostics() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("╔════════════════════════════════════════════════════════════════════════════════╗\n");
        sb.append("║             INTER-TASK GAP DIAGNOSTICS (Lightweight Analysis)                ║\n");
        sb.append("╚════════════════════════════════════════════════════════════════════════════════╝\n");

        if (perWorkerStats != null && !perWorkerStats.isEmpty()) {
            sb.append(formatPerWorkerDiagnostics());
        }

        if (perQueueStats != null && !perQueueStats.isEmpty()) {
            sb.append(formatPerQueueDiagnostics());
        }

        sb.append(formatSummary());
        return sb.toString();
    }

    /**
     * Prints comprehensive gap diagnostics for all workers and queues.
     * @deprecated Use generateDiagnostics() and write to file instead
     */
    @Deprecated
    public void printDiagnostics() {
        System.out.print(generateDiagnostics());
    }

    private String formatPerWorkerDiagnostics() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("─── Per-Worker Gap Metrics ───────────────────────────────────────────────────────\n");

        long totalTasksProcessed = 0;
        long totalExecutionNs = 0;
        long totalGapNs = 0;
        long totalPollNs = 0;
        long totalEmptyQueueNs = 0;

        int workerCount = 0;
        for (WorkerStats stats : perWorkerStats) {
            if (stats.gapDiags == null) continue;

            InterTaskGapDiagnostics gaps = stats.gapDiags;
            workerCount++;

            long tasksProcessed = gaps.tasksProcessed();
            if (tasksProcessed == 0) continue;

            totalTasksProcessed += tasksProcessed;
            totalExecutionNs += gaps.executionTimeSumNs;
            totalGapNs += gaps.interTaskGapTimeSumNs;
            totalPollNs += gaps.pollTimeSumNs;
            totalEmptyQueueNs += gaps.emptyQueueTimeSumNs;

            // Per-worker output
            sb.append(String.format("%nWorker-%d (measurement tasks: %,d):%n", workerCount - 1, tasksProcessed));
            sb.append(String.format("  Execution Time:     avg=%7.2f μs  max=%7.2f ms%n",
                gaps.executionTimeAvgNs() / 1000.0,
                gaps.executionTimeSumNs / tasksProcessed / 1_000_000.0));

            sb.append(String.format("  Inter-Task Gap:     avg=%7.2f μs  p50=%7.2f μs  p95=%7.2f μs  p99=%7.2f μs  max=%7.2f ms%n",
                gaps.interTaskGapAvgNs() / 1000.0,
                (gaps.hasHistograms() ? gaps.interTaskGapPercentile(50) : -1) / 1000.0,
                (gaps.hasHistograms() ? gaps.interTaskGapPercentile(95) : -1) / 1000.0,
                (gaps.hasHistograms() ? gaps.interTaskGapPercentile(99) : -1) / 1000.0,
                gaps.interTaskGapMaxNs() / 1_000_000.0));

            sb.append(String.format("  Poll/Dequeue Time:  avg=%7.2f μs  p50=%7.2f μs  p95=%7.2f μs  p99=%7.2f μs  max=%7.2f ms%n",
                gaps.pollTimeAvgNs() / 1000.0,
                (gaps.hasHistograms() ? gaps.pollTimePercentile(50) : -1) / 1000.0,
                (gaps.hasHistograms() ? gaps.pollTimePercentile(95) : -1) / 1000.0,
                (gaps.hasHistograms() ? gaps.pollTimePercentile(99) : -1) / 1000.0,
                gaps.pollTimeMaxNs() / 1_000_000.0));

            double emptyRatio = gaps.emptyQueueRatio();
            sb.append(String.format("  Queue State:        empty=%.1f%%  non-empty=%.1f%%  (avg empty wait: %.2f μs)%n",
                emptyRatio * 100.0, (1.0 - emptyRatio) * 100.0,
                gaps.emptyQueueTimeAvgNs() / 1000.0));
        }

        // Aggregate summary
        if (totalTasksProcessed > 0 && workerCount > 0) {
            sb.append("\n");
            sb.append(String.format("╔ Aggregate (all %d workers) ──────────────────────────────────────────────────────╗%n",
                workerCount));
            sb.append(String.format("║  Total measurement tasks: %,d%n", totalTasksProcessed));
            sb.append(String.format("║  Avg execution time:      %.2f μs  (%.1f%% of wall time)%n",
                totalExecutionNs / (double) totalTasksProcessed / 1000.0,
                100.0 * totalExecutionNs / (totalExecutionNs + totalGapNs)));
            sb.append(String.format("║  Avg inter-task gap:      %.2f μs  (%.1f%% of wall time)%n",
                totalGapNs / (double) totalTasksProcessed / 1000.0,
                100.0 * totalGapNs / (totalExecutionNs + totalGapNs)));
            sb.append(String.format("║  Avg poll time:           %.2f μs  (%.1f%% of wall time)%n",
                totalPollNs / (double) totalTasksProcessed / 1000.0,
                100.0 * totalPollNs / (totalExecutionNs + totalGapNs)));
            sb.append(String.format("║  Avg empty queue wait:    %.2f μs  (%.1f%% of poll time)%n",
                totalEmptyQueueNs / (double) totalTasksProcessed / 1000.0,
                totalPollNs > 0 ? 100.0 * totalEmptyQueueNs / totalPollNs : 0.0));
            sb.append("╚═══════════════════════════════════════════════════════════════════════════════════╝\n");
        }
        return sb.toString();
    }

    @Deprecated
    private void printPerWorkerDiagnostics() {
        System.out.print(formatPerWorkerDiagnostics());
    }

    private String formatPerQueueDiagnostics() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("─── Per-Queue Metrics ─────────────────────────────────────────────────────────────\n");

        for (QueueStatistics qstats : perQueueStats) {
            sb.append(String.format("%nQueue: %s%n", qstats.queueName()));
            sb.append(String.format("  Enqueued:     %,d%n", qstats.enqueuedCount()));
            sb.append(String.format("  Dequeued:     %,d%n", qstats.dequeuedCount()));
            sb.append(String.format("  Avg Depth:    %d%n", qstats.avgDepth()));
            sb.append(String.format("  Max Depth:    %d%n", qstats.maxDepth()));
            sb.append(String.format("  Non-Empty:    %.1f%%%n", qstats.nonEmptyRatio() * 100.0));
        }
        return sb.toString();
    }

    @Deprecated
    private void printPerQueueDiagnostics() {
        System.out.print(formatPerQueueDiagnostics());
    }

    private String formatSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("─── Key Insights ─────────────────────────────────────────────────────────────────\n");
        sb.append("\n");
        sb.append("  Inter-task gap = time between finishing one task and starting the next\n");
        sb.append("  └─ Poll Time:      time acquiring task from queue\n");
        sb.append("     ├─ Empty Queue: waiting for tasks to arrive\n");
        sb.append("     └─ Non-Empty:   dequeuing when queue already had tasks\n");
        sb.append("\n");
        sb.append("  High inter-task gap indicates:\n");
        sb.append("    • Contention on shared queue (compare Q1 vs Q32)\n");
        sb.append("    • Unbalanced workload distribution (memory vs cpu)\n");
        sb.append("    • Empty queue waits (insufficient task submission rate)\n");
        sb.append("\n");
        return sb.toString();
    }

    @Deprecated
    private void printSummary() {
        System.out.print(formatSummary());
    }
}

