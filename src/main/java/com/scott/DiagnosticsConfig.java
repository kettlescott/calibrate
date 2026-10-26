package com.scott;

/**
 * Optional diagnostics for sharded benchmarks (tail-latency / imbalance
 * analysis). Off by default: when {@code enabled=false}, no extra state
 * is allocated, no worker hot-path instrumentation runs, no background
 * sampler threads start — the benchmark numbers are byte-identical to a
 * build without this code.
 *
 * <p>All fields are documented under {@code diagnostics:} in YAML, e.g.:
 * <pre>
 * diagnostics:
 *   enabled: true
 *   perWorkerLatency: false      # HEAVY — diagnostic runs only
 *   queueDepthByShard: true      # cheap; per-shard queue depth
 *   windowSampling: true         # cheap; compact per-window summaries
 *   windowSeconds: 1             # snapshot interval
 *   slowExecutionMicros: 200     # static threshold for slow-burst counters
 * </pre>
 *
 * <h3>Cost model</h3>
 * <ul>
 *   <li><b>Always-on counters</b> (when {@code enabled=true}) — a
 *       handful of primitive long ops per task per worker. No
 *       allocation, no synchronisation.</li>
 *   <li><b>{@code perWorkerLatency=true}</b> — adds one
 *       {@link LatencyRecorder} per worker. <em>Each recorder stores
 *       four longs per task</em> (submit overhead, queue wait,
 *       execution, end-to-end). Approximate raw cost:
 *       {@code tasks × 4 × 8 bytes} plus array overhead. For a 10 M-task
 *       run that is ~320 MB total. <b>Enable only for diagnostic runs
 *       with a bounded {@code taskCount}.</b> Do NOT enable for
 *       headline throughput / latency experiments.</li>
 *   <li><b>{@code queueDepthByShard}</b> — periodic sampler thread polls
 *       shard queue sizes at 10 ms; negligible cost.</li>
 *   <li><b>{@code windowSampling}</b> — emits compact per-window
 *       summaries (min/mean/max/maxOverMin completed, max shard queue
 *       depth, queue-depth imbalance). No large per-worker arrays.</li>
 * </ul>
 *
 * <p>Sharded-mode focused: per-worker latency stats require a stable
 * worker-id → thread mapping which {@link ShardedWorker} provides
 * natively. Shared mode's JDK ThreadPoolExecutor is not instrumented
 * (would require a per-task wrapper, violating the "no per-task
 * allocation" rule).
 */
public record DiagnosticsConfig(
        boolean enabled,
        boolean perWorkerLatency,
        boolean queueDepthByShard,
        boolean windowSampling,
        boolean windowCorrelation,
        boolean shardLatencyCsv,
        boolean rawTaskLogging,
        boolean taskExecutionCsv,
        double  windowSeconds,
        long    shardWindowMillis,
        long    slowExecutionMicros
) {
    /** Disabled — zero-overhead default returned when the YAML omits the block. */
    public static DiagnosticsConfig disabled() {
        return new DiagnosticsConfig(false, false, false, false, false, false, false, false, 1.0, 100L, 200L);
    }

    /** True if any sub-flag is on; cheap top-level guard. */
    public boolean anyEnabled() {
        return enabled && (perWorkerLatency || queueDepthByShard || windowSampling
                || windowCorrelation || shardLatencyCsv || rawTaskLogging || taskExecutionCsv);
    }

    public long slowExecutionNanos() {
        return Math.max(0L, slowExecutionMicros) * 1_000L;
    }
}
