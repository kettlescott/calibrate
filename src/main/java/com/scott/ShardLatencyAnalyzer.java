package com.scott;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Per-shard / per-window latency analyzer for SHARDED runs.
 *
 * <p>Constructed only when {@link DiagnosticsConfig#shardLatencyCsv()}
 * is true. Hot path remains untouched: all per-task fields needed
 * (taskId / enqueueNanos / startNanos / finishNanos) are already on
 * {@link Task}; {@code shardId} is derived post-hoc from the same
 * formula {@link ShardedExecutor#submit(Task)} uses:
 * {@code Math.floorMod(Long.hashCode(taskId), workerCount)}.
 *
 * <h3>What this class produces</h3>
 * <ol>
 *   <li>{@code per_shard_latency.csv}        — one row per shard:
 *       processedCount + percentiles for execution / queue-wait /
 *       end-to-end (p50, p90, p95, p99, max) + avg/max queue depth.</li>
 *   <li>{@code per_window_shard_latency.csv} — one row per (window, shard):
 *       completedTasks, enqueuedTasks, throughput (tasks/s),
 *       backlogDelta = enqueuedTasks - completedTasks, execution /
 *       queue-wait / end-to-end avg+p95+p99, avg/max queue depth.</li>
 *   <li>{@code shard_window_correlation.csv} — one row per shard, six
 *       Pearson r values that disentangle backlog formation:
 *       coincident (execP95, depth_t / throughput_t / backlogDelta_t)
 *       and 1-window-lagged (execP95→depth_{t+1}, execP99→qwP99_{t+1},
 *       backlogDelta→depth_{t+1}).</li>
 * </ol>
 *
 * <h3>Background sampler</h3>
 * Runs at 10 ms cadence on a single daemon thread for the duration of
 * the submit window only. Allocations are pre-sized from the expected
 * measurement duration; the sampler never allocates per tick after
 * startup. Single-writer counters; no synchronization.
 *
 * <p>Costs scale linearly with {@code measurementSeconds × shardCount}.
 * Example: 60 s × 32 shards × 100 sample/s × 4 B = ~750 KB resident.
 */
public final class ShardLatencyAnalyzer {

    private static final long SAMPLE_INTERVAL_NS = 10_000_000L; // 10 ms

    private final int shardCount;
    private final int capacity;

    /** Absolute monotonic timestamps for each sample tick. */
    private final long[] sampleTsNs;
    /** {@code sampleDepth[shard][tick]}. Pre-allocated, never grows. */
    private final int[][] sampleDepth;
    private int sampleCount;

    private Thread thread;
    private volatile boolean running;
    private ShardedExecutor executor;

    public ShardLatencyAnalyzer(int shardCount, long expectedDurationMs) {
        this.shardCount = shardCount;
        // Pre-size with slack to avoid bounds checks / reallocation.
        // 100 samples/s + 50% headroom + small constant.
        long ticksWithSlack = (expectedDurationMs / 10L) + (expectedDurationMs / 20L) + 1024L;
        this.capacity   = (int) Math.min(Integer.MAX_VALUE, ticksWithSlack);
        this.sampleTsNs = new long[capacity];
        this.sampleDepth = new int[shardCount][capacity];
    }

    /** Starts the 10 ms-cadence per-shard depth sampler. */
    public void start(ShardedExecutor sx) {
        this.executor = sx;
        this.running  = true;
        this.thread = new Thread(this::loop, "ShardLatencyAnalyzer-sampler");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    public void stop() {
        if (thread == null) return;
        running = false;
        thread.interrupt();
        try { thread.join(2_000L); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private void loop() {
        long next = System.nanoTime();
        while (running) {
            if (sampleCount < capacity) {
                long now = System.nanoTime();
                sampleTsNs[sampleCount] = now;
                for (int s = 0; s < shardCount; s++) {
                    sampleDepth[s][sampleCount] = executor.getQueueSize(s);
                }
                sampleCount++;
            }
            next += SAMPLE_INTERVAL_NS;
            long sleep = next - System.nanoTime();
            if (sleep > 0) LockSupport.parkNanos(sleep);
            else            next = System.nanoTime();
        }
    }

    /* =================================================================
     *  Post-run analysis + CSV emission
     * ================================================================= */

    /**
     * Buckets the measurement tasks per shard and per window, computes
     * percentiles and Pearson correlations, and writes 3 CSV files to
     * {@code runDir}. Allocation-free for hot-path readers — all sorting
     * and statistics happen here on the main thread after the run.
     */
    public void analyzeAndWrite(Path runDir,
                                List<Task> tasks,
                                int workerCount,
                                long submitStartNs,
                                long submitEndNs,
                                long windowMillis,
                                boolean writeAggregateCsvs,
                                boolean rawTaskLogging) throws IOException {
        analyzeAndWrite(runDir, tasks, workerCount, submitStartNs, submitEndNs,
                windowMillis, writeAggregateCsvs, rawTaskLogging,
                ShardedRoutingConfig.defaults());
    }

    /**
     * Routing-aware overload. {@code routing} must match
     * the {@link ShardedExecutor}'s submit-time routing so post-hoc
     * shard derivation agrees with on-line routing.
     */
    public void analyzeAndWrite(Path runDir,
                                List<Task> tasks,
                                int workerCount,
                                long submitStartNs,
                                long submitEndNs,
                                long windowMillis,
                                boolean writeAggregateCsvs,
                                boolean rawTaskLogging,
                                ShardedRoutingConfig routing) throws IOException {
        final ShardedRoutingConfig effRouting =
                routing == null ? ShardedRoutingConfig.defaults() : routing;
        if (workerCount != shardCount) {
            throw new IllegalArgumentException(
                    "workerCount=" + workerCount + " != shardCount=" + shardCount);
        }
        final long windowNs    = Math.max(1_000_000L, windowMillis * 1_000_000L);
        final long submitDurNs = Math.max(1L, submitEndNs - submitStartNs);
        final int  windowCount = (int) Math.max(1, (submitDurNs + windowNs - 1) / windowNs);

        // ---- 1. Bucket tasks by (shard, window) ----
        // Per-shard accumulators of all valid task latencies.
        long[][] execByShard = new long[shardCount][];
        long[][] qwByShard   = new long[shardCount][];
        long[][] e2eByShard  = new long[shardCount][];
        int[]    shardSize   = new int[shardCount];

        // Per (shard, window) accumulators — sized after a first count pass.
        int[][]  swCount    = new int[shardCount][windowCount];
        // Tasks ENQUEUED into this shard during this window (arrival
        // side, independent of completion). Bucketed by enqueueNanos
        // so we can derive arrival rate, throughput, and backlogDelta
        // = enqueuedTasks - completedTasks per (shard, window).
        int[][]  swEnqueued = new int[shardCount][windowCount];
        long[][] swExec   = new long[shardCount][];   // contiguous storage; flat per shard
        long[][] swQw     = new long[shardCount][];
        long[][] swE2E    = new long[shardCount][];

        // First pass: bucket counts (completion-bucket + enqueue-bucket).
        for (Task t : tasks) {
            long s = t.startNanos();
            long f = t.finishNanos();
            if (s == 0L || f == 0L) continue;
            int shard = Hashing.shardOf(t.taskId(), workerCount, effRouting);
            shardSize[shard]++;
            int win = (int) Math.min(windowCount - 1L, (s - submitStartNs) / windowNs);
            if (win < 0) win = 0;
            swCount[shard][win]++;

            // Bucket by enqueue time — measurement tasks always have
            // enqueueNanos in [submitStartNs, submitEndNs], so clamping
            // is just defensive.
            long enq = t.enqueuedNanos();
            if (enq > 0L) {
                int winEnq = (int) Math.min(windowCount - 1L, (enq - submitStartNs) / windowNs);
                if (winEnq < 0) winEnq = 0;
                swEnqueued[shard][winEnq]++;
            }
        }

        for (int sh = 0; sh < shardCount; sh++) {
            execByShard[sh] = new long[shardSize[sh]];
            qwByShard[sh]   = new long[shardSize[sh]];
            e2eByShard[sh]  = new long[shardSize[sh]];
            int total = 0;
            for (int w = 0; w < windowCount; w++) total += swCount[sh][w];
            swExec[sh] = new long[total];
            swQw[sh]   = new long[total];
            swE2E[sh]  = new long[total];
        }

        // Per-shard write cursors + per-(shard, window) cursors derived
        // from a prefix-sum so flat-array layout is cache-friendly.
        int[]   shardCursor = new int[shardCount];
        int[][] swOffset    = new int[shardCount][windowCount];
        for (int sh = 0; sh < shardCount; sh++) {
            int off = 0;
            for (int w = 0; w < windowCount; w++) {
                swOffset[sh][w] = off;
                off += swCount[sh][w];
            }
        }
        int[][] swCursor = new int[shardCount][windowCount];

        // Second pass: fill the arrays.
        for (Task t : tasks) {
            long s = t.startNanos();
            long f = t.finishNanos();
            if (s == 0L || f == 0L) continue;
            int shard = Hashing.shardOf(t.taskId(), workerCount, effRouting);
            long enq = t.enqueuedNanos();
            long qw  = enq == 0L ? 0L : (s - enq);
            long ex  = f - s;
            long e2e = enq == 0L ? (f - s) : (f - enq);

            int ci = shardCursor[shard]++;
            execByShard[shard][ci] = ex;
            qwByShard[shard][ci]   = qw;
            e2eByShard[shard][ci]  = e2e;

            int win = (int) Math.min(windowCount - 1L, (s - submitStartNs) / windowNs);
            if (win < 0) win = 0;
            int swi = swOffset[shard][win] + swCursor[shard][win]++;
            swExec[shard][swi] = ex;
            swQw[shard][swi]   = qw;
            swE2E[shard][swi]  = e2e;
        }

        // ---- 2. Bucket queue-depth samples by window ----
        // For each (window, shard): sumDepth, sampleCount, maxDepth.
        long[][] winDepthSum   = new long[shardCount][windowCount];
        int[][]  winDepthCount = new int[shardCount][windowCount];
        int[][]  winDepthMax   = new int[shardCount][windowCount];

        for (int i = 0; i < sampleCount; i++) {
            long ts = sampleTsNs[i];
            if (ts < submitStartNs || ts >= submitEndNs) continue;
            int win = (int) Math.min(windowCount - 1L, (ts - submitStartNs) / windowNs);
            if (win < 0) continue;
            for (int sh = 0; sh < shardCount; sh++) {
                int d = sampleDepth[sh][i];
                winDepthSum[sh][win]   += d;
                winDepthCount[sh][win] += 1;
                if (d > winDepthMax[sh][win]) winDepthMax[sh][win] = d;
            }
        }

        // ---- 3. Write per_shard_latency.csv ----
        Path p1 = runDir.resolve("per_shard_latency.csv");
        StringBuilder sb = new StringBuilder(4096);
        sb.append("shardId,processedCount,")
          .append("execMs_p50,execMs_p90,execMs_p95,execMs_p99,execMs_max,")
          .append("qwMs_p50,qwMs_p90,qwMs_p95,qwMs_p99,qwMs_max,")
          .append("e2eMs_p50,e2eMs_p90,e2eMs_p95,e2eMs_p99,e2eMs_max,")
          .append("avgQueueDepth,maxQueueDepth\n");
        for (int sh = 0; sh < shardCount; sh++) {
            long[] ex = execByShard[sh].clone(); Arrays.sort(ex);
            long[] qw = qwByShard[sh].clone();   Arrays.sort(qw);
            long[] e2 = e2eByShard[sh].clone();  Arrays.sort(e2);
            double[] shardDepthStats = aggregateShardDepth(winDepthSum[sh], winDepthCount[sh], winDepthMax[sh]);
            sb.append(sh).append(',').append(ex.length).append(',');
            appendPercentilesMs(sb, ex);
            appendPercentilesMs(sb, qw);
            appendPercentilesMs(sb, e2);
            sb.append(String.format("%.3f", shardDepthStats[0])).append(',')
              .append((int) shardDepthStats[1]).append('\n');
        }
        if (writeAggregateCsvs) {
            Files.writeString(p1, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        // ---- 4. Write per_window_shard_latency.csv ----
        Path p2 = runDir.resolve("per_window_shard_latency.csv");
        sb.setLength(0);
        sb.append("windowStartMs,shardId,completedTasks,enqueuedTasks,")
          .append("throughputTasksPerSec,backlogDelta,")
          .append("avgExecMs,p95ExecMs,p99ExecMs,")
          .append("avgQwMs,p95QwMs,p99QwMs,")
          .append("avgE2EMs,p95E2EMs,p99E2EMs,")
          .append("avgQueueDepth,maxQueueDepth\n");
        final double windowSeconds = windowMillis / 1000.0;
        for (int w = 0; w < windowCount; w++) {
            long startMs = (w * windowNs) / 1_000_000L;
            for (int sh = 0; sh < shardCount; sh++) {
                int n        = swCount[sh][w];
                int enqueued = swEnqueued[sh][w];
                int off      = swOffset[sh][w];
                double throughput  = n / windowSeconds;       // tasks / sec
                int    backlogDelta = enqueued - n;           // arrivals − completions
                double avgEx, p95Ex, p99Ex, avgQw, p95Qw, p99Qw, avgE2, p95E2, p99E2;
                if (n == 0) {
                    avgEx = p95Ex = p99Ex = avgQw = p95Qw = p99Qw =
                            avgE2 = p95E2 = p99E2 = 0.0;
                } else {
                    long[] exSlice = Arrays.copyOfRange(swExec[sh], off, off + n);
                    long[] qwSlice = Arrays.copyOfRange(swQw[sh],   off, off + n);
                    long[] e2Slice = Arrays.copyOfRange(swE2E[sh],  off, off + n);
                    Arrays.sort(exSlice); Arrays.sort(qwSlice); Arrays.sort(e2Slice);
                    avgEx = mean(exSlice) / 1e6;
                    p95Ex = percentile(exSlice, 95) / 1e6;
                    p99Ex = percentile(exSlice, 99) / 1e6;
                    avgQw = mean(qwSlice) / 1e6;
                    p95Qw = percentile(qwSlice, 95) / 1e6;
                    p99Qw = percentile(qwSlice, 99) / 1e6;
                    avgE2 = mean(e2Slice) / 1e6;
                    p95E2 = percentile(e2Slice, 95) / 1e6;
                    p99E2 = percentile(e2Slice, 99) / 1e6;
                }
                double avgD = winDepthCount[sh][w] == 0
                        ? 0.0 : winDepthSum[sh][w] / (double) winDepthCount[sh][w];
                int    maxD = winDepthMax[sh][w];
                sb.append(startMs).append(',').append(sh).append(',')
                  .append(n).append(',').append(enqueued).append(',')
                  .append(String.format("%.1f,", throughput))
                  .append(backlogDelta).append(',')
                  .append(String.format("%.3f,%.3f,%.3f,", avgEx, p95Ex, p99Ex))
                  .append(String.format("%.3f,%.3f,%.3f,", avgQw, p95Qw, p99Qw))
                  .append(String.format("%.3f,%.3f,%.3f,", avgE2, p95E2, p99E2))
                  .append(String.format("%.3f", avgD)).append(',')
                  .append(maxD).append('\n');
            }
        }
        if (writeAggregateCsvs) {
            Files.writeString(p2, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        // ---- 5. Write shard_window_correlation.csv ----
        // Per-shard, the lagged correlations that distinguish A/B/C/D:
        //   r(execP95_t      , depth_t)        ← coincident
        //   r(execP95_t      , depth_{t+1})    ← execution leads backlog
        //   r(execP99_t      , qwP99_{t+1})    ← execution leads observed queue wait
        //   r(execP95_t      , throughput_t)   ← slower exec → lower throughput same window
        //   r(execP95_t      , backlogDelta_t) ← slower exec → arrivals outpace completions
        //   r(backlogDelta_t , depth_{t+1})    ← arrivals - completions predicts next depth
        Path p3 = runDir.resolve("shard_window_correlation.csv");
        sb.setLength(0);
        sb.append("shardId,windows,")
          .append("corr_execP95_vs_depth_t,")
          .append("corr_execP95_vs_depth_tPlus1,")
          .append("corr_execP99_vs_qwP99_tPlus1,")
          .append("corr_execP95_vs_throughput_t,")
          .append("corr_execP95_vs_backlogDelta_t,")
          .append("corr_backlogDelta_vs_depth_tPlus1\n");
        for (int sh = 0; sh < shardCount; sh++) {
            double[] execP95     = new double[windowCount];
            double[] execP99     = new double[windowCount];
            double[] qwP99       = new double[windowCount];
            double[] depth       = new double[windowCount];
            double[] throughput  = new double[windowCount];
            double[] backlogDelta = new double[windowCount];
            for (int w = 0; w < windowCount; w++) {
                int n = swCount[sh][w];
                int off = swOffset[sh][w];
                if (n > 0) {
                    long[] exSlice = Arrays.copyOfRange(swExec[sh], off, off + n);
                    long[] qwSlice = Arrays.copyOfRange(swQw[sh],   off, off + n);
                    Arrays.sort(exSlice); Arrays.sort(qwSlice);
                    execP95[w] = percentile(exSlice, 95);
                    execP99[w] = percentile(exSlice, 99);
                    qwP99[w]   = percentile(qwSlice, 99);
                }
                depth[w] = winDepthCount[sh][w] == 0
                        ? 0.0 : winDepthSum[sh][w] / (double) winDepthCount[sh][w];
                throughput[w]   = n / windowSeconds;
                backlogDelta[w] = swEnqueued[sh][w] - n;
            }
            double rA = pearson(execP95, depth);                          // coincident
            double rB = pearsonLagged(execP95, depth, 1);                 // depth lags execution
            double rC = pearsonLagged(execP99, qwP99, 1);                 // qwait lags execution
            double rD = pearson(execP95, throughput);                     // exec ↔ throughput
            double rE = pearson(execP95, backlogDelta);                   // exec ↔ backlog growth
            double rF = pearsonLagged(backlogDelta, depth, 1);            // depth lags backlog
            sb.append(sh).append(',').append(windowCount).append(',')
              .append(fmt(rA)).append(',').append(fmt(rB)).append(',').append(fmt(rC)).append(',')
              .append(fmt(rD)).append(',').append(fmt(rE)).append(',').append(fmt(rF))
              .append('\n');
        }
        if (writeAggregateCsvs) {
            Files.writeString(p3, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        // ---- 6. Optional per-task CSV (rawTaskLogging=true) ----
        // Heavy: one row per measurement task. Stream-written via
        // BufferedWriter to avoid building a multi-million-row String
        // in memory. Off by default; intended for bounded debug runs.
        if (rawTaskLogging) {
            Path p4 = runDir.resolve("per_task.csv");
            try (var w = Files.newBufferedWriter(p4, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                w.write("taskId,shardId,workerId,enqueueNanos,startNanos,finishNanos,"
                        + "queueWaitNanos,executionNanos,endToEndNanos\n");
                StringBuilder row = new StringBuilder(128);
                for (Task t : tasks) {
                    long s = t.startNanos();
                    long f = t.finishNanos();
                    if (s == 0L || f == 0L) continue;
                    int shard = Hashing.shardOf(t.taskId(), workerCount, effRouting);
                    long enq = t.enqueuedNanos();
                    long qw  = enq == 0L ? 0L : (s - enq);
                    long ex  = f - s;
                    long e2e = enq == 0L ? ex : (f - enq);
                    row.setLength(0);
                    row.append(t.taskId()).append(',')
                       .append(shard).append(',')
                       .append(shard).append(',')          // workerId == shardId in SHARDED
                       .append(enq).append(',')
                       .append(s).append(',')
                       .append(f).append(',')
                       .append(qw).append(',')
                       .append(ex).append(',')
                       .append(e2e).append('\n');
                    w.write(row.toString());
                }
            }
        }
    }

    /* ---------------- helpers ---------------- */

    /** Returns {avg, max} of the per-window queue depth for one shard. */
    private static double[] aggregateShardDepth(long[] sum, int[] count, int[] max) {
        long totalSum = 0; long totalCount = 0; int overallMax = 0;
        for (int i = 0; i < sum.length; i++) {
            totalSum   += sum[i];
            totalCount += count[i];
            if (max[i] > overallMax) overallMax = max[i];
        }
        double avg = totalCount == 0 ? 0.0 : totalSum / (double) totalCount;
        return new double[] { avg, overallMax };
    }

    private static void appendPercentilesMs(StringBuilder sb, long[] sortedNs) {
        if (sortedNs.length == 0) {
            sb.append("0,0,0,0,0,");
            return;
        }
        sb.append(String.format("%.3f,", percentile(sortedNs, 50) / 1e6));
        sb.append(String.format("%.3f,", percentile(sortedNs, 90) / 1e6));
        sb.append(String.format("%.3f,", percentile(sortedNs, 95) / 1e6));
        sb.append(String.format("%.3f,", percentile(sortedNs, 99) / 1e6));
        sb.append(String.format("%.3f,", sortedNs[sortedNs.length - 1] / 1e6));
    }

    /** Nearest-rank percentile on a pre-sorted long[]. */
    private static double percentile(long[] sorted, int pct) {
        if (sorted.length == 0) return 0.0;
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sorted.length) idx = sorted.length - 1;
        return sorted[idx];
    }

    private static double mean(long[] data) {
        if (data.length == 0) return 0.0;
        long sum = 0; for (long v : data) sum += v;
        return (double) sum / data.length;
    }

    /** Pearson r over equal-length vectors; NaN for degenerate input. */
    private static double pearson(double[] xs, double[] ys) {
        return pearsonLagged(xs, ys, 0);
    }

    /**
     * Pearson r where {@code ys} is shifted forward by {@code lag} windows.
     * That is, we correlate {@code (xs[t], ys[t+lag])} for valid t.
     */
    private static double pearsonLagged(double[] xs, double[] ys, int lag) {
        int n = Math.min(xs.length, ys.length - lag);
        if (n < 2) return Double.NaN;
        double sx = 0, sy = 0;
        for (int i = 0; i < n; i++) { sx += xs[i]; sy += ys[i + lag]; }
        double mx = sx / n, my = sy / n;
        double num = 0, dx2 = 0, dy2 = 0;
        for (int i = 0; i < n; i++) {
            double dx = xs[i]       - mx;
            double dy = ys[i + lag] - my;
            num += dx * dy;
            dx2 += dx * dx;
            dy2 += dy * dy;
        }
        if (dx2 == 0.0 || dy2 == 0.0) return Double.NaN;
        return num / Math.sqrt(dx2 * dy2);
    }

    private static String fmt(double r) {
        return Double.isNaN(r) ? "nan" : String.format("%.3f", r);
    }
}

