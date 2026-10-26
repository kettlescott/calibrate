package com.scott;

import java.util.EnumMap;
import java.util.Map;

/**
 * Per-{@link WorkloadKind} latency aggregator.
 *
 * <p>Maintains one {@link LatencyRecorder} per kind plus an overall
 * recorder, so the summary can report queue-wait / execution /
 * end-to-end percentiles (p50/p95/p99) for each kind independently --
 * essential when comparing routing policies whose effect depends on
 * what kind of task each queue receives.
 */
public final class PerKindLatencyRecorder {

    private final LatencyRecorder overall;
    private final Map<WorkloadKind, LatencyRecorder> byKind;

    public PerKindLatencyRecorder(int expectedTasks) {
        this.overall = new LatencyRecorder(expectedTasks);
        this.byKind = new EnumMap<>(WorkloadKind.class);
        int perKindCapacity = Math.max(64, expectedTasks / Math.max(1, WorkloadKind.values().length));
        for (WorkloadKind k : WorkloadKind.values()) {
            byKind.put(k, new LatencyRecorder(perKindCapacity));
        }
    }

    public void record(Task task) {
        overall.record(task);
        byKind.get(task.workloadKind()).record(task);
    }

    /** Records primitive samples for overall + per-kind buckets. */
    public void recordRaw(WorkloadKind kind, long so, long qw, long ex, long e2e) {
        overall.recordRaw(so, qw, ex, e2e);
        byKind.get(kind).recordRaw(so, qw, ex, e2e);
    }

    public int recordedTasks() { return overall.recordedTasks(); }

    public LatencyRecorder overall() { return overall; }
    public LatencyRecorder forKind(WorkloadKind kind) { return byKind.get(kind); }

    /** Multi-section summary: overall first, then one section per kind that has &gt; 0 samples. */
    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Latency: overall ===\n");
        sb.append(overall.summary());

        for (WorkloadKind k : WorkloadKind.values()) {
            LatencyRecorder r = byKind.get(k);
            if (r.recordedTasks() == 0) continue;
            sb.append('\n');
            sb.append("=== Latency: ").append(k.name()).append(" ===\n");
            sb.append(r.summary());
        }
        return sb.toString();
    }

    /** Compact one-line-per-metric percentile dump for the summary file. */
    public String compactByKind() {
        StringBuilder sb = new StringBuilder();
        for (WorkloadKind k : WorkloadKind.values()) {
            LatencyRecorder r = byKind.get(k);
            if (r.recordedTasks() == 0) continue;
            sb.append(String.format("perKind.%s.count=%d%n", k.name(), r.recordedTasks()));
            sb.append(String.format(
                    "perKind.%s.queueWaitMs.p50=%.3f, p95=%.3f, p99=%.3f%n",
                    k.name(),
                    r.p50QueueWait() / 1_000_000.0,
                    r.p95QueueWait() / 1_000_000.0,
                    r.p99QueueWait() / 1_000_000.0));
            sb.append(String.format(
                    "perKind.%s.executionMs.avg=%.3f%n",
                    k.name(),
                    r.avgExecution() / 1_000_000.0));
            sb.append(String.format(
                    "perKind.%s.executionMs.p50=%.3f, p95=%.3f, p99=%.3f%n",
                    k.name(),
                    r.p50Execution() / 1_000_000.0,
                    r.p95Execution() / 1_000_000.0,
                    r.p99Execution() / 1_000_000.0));
            sb.append(String.format(
                    "perKind.%s.endToEndMs.p50=%.3f, p95=%.3f, p99=%.3f%n",
                    k.name(),
                    r.p50EndToEnd() / 1_000_000.0,
                    r.p95EndToEnd() / 1_000_000.0,
                    r.p99EndToEnd() / 1_000_000.0));
        }
        return sb.toString();
    }
}

