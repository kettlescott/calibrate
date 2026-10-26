package com.scott;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Generic diagnostics collector — works for SHARED and SHARDED
 * topologies. Constructed only when {@link DiagnosticsConfig#anyEnabled()};
 * when disabled, the benchmark never touches this code.
 *
 * <h3>Topology mapping</h3>
 * <ul>
 *   <li><b>SHARDED</b>: {@code workerCount} = number of shards;
 *       {@code queueId == workerId}. The {@link QueueProbe} is wired to
 *       {@code ShardedExecutor::getQueueSize}.</li>
 *   <li><b>SHARED</b>:  {@code workerCount = 1}. All pool threads
 *       feed a single aggregate {@link WorkerStats} entry; the
 *       {@link QueueProbe} is wired to {@code SharedExecutor::getQueueSize}.
 *       Output uses {@code queueId=0} / {@code worker[0]} as the
 *       user-facing labels.</li>
 *   <li><b>HYBRID</b>:  not auto-enabled in this revision; documented
 *       as a follow-up. The diagnostics block simply does not appear
 *       for HYBRID runs.</li>
 * </ul>
 *
 * <h3>Hot-path cost</h3>
 * Workers update worker-local {@link WorkerStats} via plain long ops
 * (no atomics, no synchronized). The sampler thread reads
 * {@code publishedProcessed} (volatile) and accepts the documented
 * statistical-only tear risk on the other plain-long fields
 * (execSumNs, queueWaitSumNs, slowTotal, slowStreakMax) — Pearson
 * correlation across &gt;= 60 windows is unaffected by &lt; 1-in-2^32
 * tear events on 64-bit JVMs.
 */
public final class DiagnosticsCollector {

    /**
     * Tiny abstraction over "queue at index i" so this collector is
     * indifferent to executor topology. Plug in
     * {@code ShardedExecutor::getQueueSize} or
     * {@code SharedExecutor::getQueueSize} (with a single index=0).
     */
    @FunctionalInterface
    public interface QueueProbe {
        /** Returns the current depth of queue {@code queueId}. */
        int queueDepth(int queueId);
    }

    private final DiagnosticsConfig cfg;
    private final int workerCount;
    /** Number of distinct queues (==workerCount in SHARDED, 1 in SHARED). */
    private final int queueCount;
    private final WorkerStats[] stats;

    /* ----- Per-window snapshots (populated only when windowSampling=true) -----
     * Cumulative counters are sampled at each window boundary; deltas
     * vs. the previous snapshot are stashed as fresh long[] / int[]
     * arrays. No per-worker per-window object allocation beyond these
     * fixed-shape arrays. */
    private final List<long[]>  windowProcessed   = new ArrayList<>();   // per-worker delta
    private final List<long[]>  windowExecSumNs   = new ArrayList<>();   // per-worker delta
    private final List<long[]>  windowQwSumNs     = new ArrayList<>();   // per-worker delta
    private final List<long[]>  windowSlowDelta   = new ArrayList<>();   // per-worker delta (slowTotal)
    private final List<long[]>  windowMaxBurstDelta = new ArrayList<>(); // per-worker delta (maxSlowBurst)
    private final List<int[]>   windowMaxDepth    = new ArrayList<>();   // per-queue max depth

    private Thread samplerThread;
    private volatile boolean samplerRunning;
    private QueueProbe samplerProbe;

    /* Pre-allocated scratch for the sampler (no per-tick alloc). */
    private long[] prevProcessed;
    private long[] prevExecSum;
    private long[] prevQwSum;
    private long[] prevSlowTotal;
    private long[] prevMaxBurst;

    /* --- Per-queue sub-tick occupancy counters (run-lifetime totals) ---
     * Updated once per 10 ms sub-tick by the sampler thread only,
     * read after stop(). Answers: "what fraction of the run was
     * queue[i] empty?" — a proxy for local starvation. */
    private long[] tickTotal;   // total sub-ticks observed per queue
    private long[] tickEmpty;   // sub-ticks where depth == 0
    private long[] depthSum;    // sum of depth across all sub-ticks (for true avg)

    public DiagnosticsCollector(DiagnosticsConfig cfg, int workerCount, int expectedTotalTasksHint) {
        this(cfg, workerCount, workerCount, expectedTotalTasksHint);
    }

    /**
     * @param workerCount number of distinct {@link WorkerStats} buckets
     *                    (== shard count in SHARDED; 1 in SHARED).
     * @param queueCount  number of distinct queues sampled by the
     *                    {@link QueueProbe} (== shard count in SHARDED;
     *                    1 in SHARED). Usually equals workerCount.
     */
    public DiagnosticsCollector(DiagnosticsConfig cfg, int workerCount, int queueCount,
                                int expectedTotalTasksHint) {
        this.cfg = cfg;
        this.workerCount = workerCount;
        this.queueCount  = queueCount;
        this.stats = new WorkerStats[workerCount];
        int perWorkerHint = Math.max(1024, expectedTotalTasksHint / Math.max(1, workerCount));
        for (int i = 0; i < workerCount; i++) {
            stats[i] = new WorkerStats(cfg.slowExecutionNanos(), cfg.perWorkerLatency(), perWorkerHint);
        }
    }

    public WorkerStats[] workerStats() { return stats; }

    /**
     * Sum of {@link WorkerStats#publishedProcessed} across all workers.
     * Used by the post-drain handshake in {@code BenchmarkMain} to wait
     * until every measurement task's diagnostics update has landed
     * before {@code appendSummary()} reads the counters. Volatile reads
     * give a consistent total without locking.
     */
    public long measurementProcessedTotal() {
        long total = 0L;
        for (WorkerStats s : stats) {
            total += s.publishedProcessed;
        }
        return total;
    }

    /**
     * Publishes the final per-worker processed count, ensuring the
     * tail of the run (the last &lt; 1024 measurement tasks per worker
     * that did not trigger a periodic publish) is visible to readers.
     * Called by {@code BenchmarkMain} after the post-drain handshake.
     */
    public void publishFinalCounts() {
        for (WorkerStats s : stats) {
            s.publishFinal();
        }
    }

    /* ================================================================
     *  Background window sampler
     * ================================================================ */

    /** Starts the periodic sampler. No-op if windowSampling is disabled. */
    public void startSampler(ShardedExecutor executor) {
        startSampler(executor::getQueueSize);
    }

    /** Topology-agnostic entry point. Used by SHARED-mode wiring. */
    public void startSampler(QueueProbe probe) {
        if (!cfg.windowSampling()) return;
        this.samplerProbe   = probe;
        this.prevProcessed  = new long[workerCount];
        this.prevExecSum    = new long[workerCount];
        this.prevQwSum      = new long[workerCount];
        this.prevSlowTotal  = new long[workerCount];
        this.prevMaxBurst   = new long[workerCount];
        this.tickTotal      = new long[queueCount];
        this.tickEmpty      = new long[queueCount];
        this.depthSum       = new long[queueCount];
        this.samplerRunning = true;
        this.samplerThread = new Thread(this::samplerLoop, "DiagnosticsSampler");
        this.samplerThread.setDaemon(true);
        this.samplerThread.start();
    }

    public void stopSampler() {
        if (samplerThread == null) return;
        samplerRunning = false;
        samplerThread.interrupt();
        try {
            samplerThread.join(2_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void samplerLoop() {
        // windowSeconds may be a sub-second decimal (e.g. 0.5, 0.1).
        // Floor at 1 ms so the sampler can never become a busy loop.
        final long intervalNanos = Math.max(1_000_000L,
                (long) (cfg.windowSeconds() * 1_000_000_000.0));
        // High-resolution sub-tick: poll queue depth at 10 ms so we
        // capture max depth between window boundaries without per-tick
        // allocation. Tracked per-queue, reset at each window roll-over.
        final long subTickNanos = 10_000_000L; // 10 ms
        int[] curMaxDepth = new int[queueCount];

        long windowEnd  = System.nanoTime() + intervalNanos;
        long nextSubTick = System.nanoTime();
        while (samplerRunning) {
            // Sub-tick: refresh per-queue max depth AND accumulate
            // run-lifetime occupancy stats (avg depth, empty fraction).
            for (int q = 0; q < queueCount; q++) {
                int d = samplerProbe.queueDepth(q);
                if (d > curMaxDepth[q]) curMaxDepth[q] = d;
                tickTotal[q]++;
                depthSum[q] += d;
                if (d == 0) tickEmpty[q]++;
            }

            long now = System.nanoTime();
            if (now >= windowEnd) {
                // Window rollover: snapshot every cumulative WorkerStats
                // counter and compute deltas. Plain long reads on
                // execSumNs/queueWaitSumNs/slowTotal/slowStreakMax — see
                // class javadoc for the tear-tolerance argument.
                long[] dProc      = new long[workerCount];
                long[] dExec      = new long[workerCount];
                long[] dQw        = new long[workerCount];
                long[] dSlow      = new long[workerCount];
                long[] dMaxBurst  = new long[workerCount];
                for (int w = 0; w < workerCount; w++) {
                    WorkerStats s = stats[w];
                    long p  = s.publishedProcessed;     // volatile read
                    long e  = s.execSumNs;              // plain (statistical)
                    long q  = s.queueWaitSumNs;         // plain (statistical)
                    long st = s.slowTotal;              // plain (statistical)
                    long mb = s.slowStreakMax;          // plain (statistical)
                    dProc[w]     = p  - prevProcessed[w];  prevProcessed[w]  = p;
                    dExec[w]     = e  - prevExecSum[w];    prevExecSum[w]    = e;
                    dQw[w]       = q  - prevQwSum[w];      prevQwSum[w]      = q;
                    dSlow[w]     = st - prevSlowTotal[w];  prevSlowTotal[w]  = st;
                    dMaxBurst[w] = mb - prevMaxBurst[w];   prevMaxBurst[w]   = mb;
                }
                windowProcessed.add(dProc);
                windowExecSumNs.add(dExec);
                windowQwSumNs.add(dQw);
                windowSlowDelta.add(dSlow);
                windowMaxBurstDelta.add(dMaxBurst);
                windowMaxDepth.add(curMaxDepth);
                curMaxDepth = new int[queueCount];
                windowEnd += intervalNanos;
            }

            nextSubTick += subTickNanos;
            long sleep = nextSubTick - System.nanoTime();
            if (sleep > 0) LockSupport.parkNanos(sleep);
            else           nextSubTick = System.nanoTime();
        }
    }

    /* ================================================================
     *  Summary
     * ================================================================ */

    /**
     * Appends the diagnostics blocks to summary_sharded50.txt. Called after
     * worker threads have processed all measurement tasks.
     */
    public void appendSummary(StringBuilder sb) {
        // ---- Config block (so the run is self-describing) ----
        sb.append('\n').append("=== Diagnostics: config ===\n");
        sb.append("diagnostics.enabled=").append(cfg.enabled()).append('\n');
        sb.append("diagnostics.perWorkerLatency=").append(cfg.perWorkerLatency()).append('\n');
        sb.append("diagnostics.queueDepthByShard=").append(cfg.queueDepthByShard()).append('\n');
        sb.append("diagnostics.windowSampling=").append(cfg.windowSampling()).append('\n');
        sb.append("diagnostics.windowCorrelation=").append(cfg.windowCorrelation()).append('\n');
        sb.append("diagnostics.windowSeconds=").append(cfg.windowSeconds()).append('\n');
        sb.append("diagnostics.slowExecutionMicros=").append(cfg.slowExecutionMicros()).append('\n');

        // ---- Per-worker block ----
        sb.append('\n').append("=== Diagnostics: per worker ===\n");
        for (int i = 0; i < workerCount; i++) {
            WorkerStats s = stats[i];
            sb.append(String.format("worker[%d].processed=%d%n", i, s.processed));
            sb.append(String.format("worker[%d].slowTasks=%d%n",     i, s.slowTotal));
            sb.append(String.format("worker[%d].maxSlowBurst=%d%n",  i, s.slowStreakMax));
            if (s.histogram != null && s.processed > 0) {
                double avgExecMs = (s.execSumNs / (double) s.processed) / 1_000_000.0;
                double avgQwMs   = (s.queueWaitSumNs / (double) s.processed) / 1_000_000.0;
                LatencyRecorder h = s.histogram;
                sb.append(String.format(
                        "worker[%d].executionMs.avg=%.3f, p50=%.3f, p95=%.3f, p99=%.3f, max=%.3f%n",
                        i, avgExecMs,
                        h.p50Execution()/1e6, h.p95Execution()/1e6, h.p99Execution()/1e6,
                        (s.execMaxNs == Long.MIN_VALUE ? 0 : s.execMaxNs) / 1e6));
                sb.append(String.format(
                        "worker[%d].queueWaitMs.avg=%.3f, p50=%.3f, p95=%.3f, p99=%.3f, max=%.3f%n",
                        i, avgQwMs,
                        h.p50QueueWait()/1e6, h.p95QueueWait()/1e6, h.p99QueueWait()/1e6,
                        (s.queueWaitMaxNs == Long.MIN_VALUE ? 0 : s.queueWaitMaxNs) / 1e6));
            }
        }

        // ---- Per-queue depth block ----
        if (cfg.queueDepthByShard()) {
            sb.append('\n').append("=== Diagnostics: queue depth ===\n");
            int nWindows = windowMaxDepth.size();
            double[] avgPerQueue = new double[queueCount];
            int[]    maxPerQueue = new int[queueCount];
            double[] trueAvgPerQueue  = new double[queueCount];
            double[] emptyRatioArr    = new double[queueCount];
            double[] nonEmptyRatioArr = new double[queueCount];
            long aggTotal = 0L, aggEmpty = 0L, aggDepthSum = 0L;
            for (int q = 0; q < queueCount; q++) {
                long sum = 0;
                int  overallMax = 0;
                int[] perWin = new int[nWindows];
                for (int w = 0; w < nWindows; w++) {
                    int d = windowMaxDepth.get(w)[q];
                    perWin[w] = d;
                    sum += d;
                    if (d > overallMax) overallMax = d;
                }
                double avg = nWindows == 0 ? 0.0 : (double) sum / nWindows;
                int p95 = percentileInt(perWin, 95);
                avgPerQueue[q] = avg;
                maxPerQueue[q] = overallMax;

                // True-average depth + empty ratio from 10 ms sub-tick
                // samples (much finer-grained than the per-window max).
                long tt = tickTotal == null ? 0L : tickTotal[q];
                long te = tickEmpty == null ? 0L : tickEmpty[q];
                long ds = depthSum  == null ? 0L : depthSum[q];
                double trueAvg = tt == 0L ? 0.0 : (double) ds / tt;
                double emptyR  = tt == 0L ? 0.0 : (double) te / tt;
                trueAvgPerQueue[q]  = trueAvg;
                emptyRatioArr[q]    = emptyR;
                nonEmptyRatioArr[q] = 1.0 - emptyR;
                aggTotal    += tt;
                aggEmpty    += te;
                aggDepthSum += ds;

                sb.append(String.format(
                        "shard[%d].avgQueueDepth=%.2f, maxQueueDepth=%d, p95QueueDepth=%d, "
                        + "trueAvgDepth=%.3f, emptyRatio=%.4f, nonEmptyRatio=%.4f  "
                        + "(windows=%d, subTicks=%d)%n",
                        q, avg, overallMax, p95, trueAvg, emptyR, 1.0 - emptyR, nWindows, tt));
            }
            sb.append(String.format("avgQueueDepth.maxOverMean=%.3f%n", maxOverMean(avgPerQueue)));
            sb.append(String.format("maxQueueDepth.maxOverMean=%.3f%n", maxOverMean(toDoubles(maxPerQueue))));

            // Aggregate summary across shards.
            double aggEmptyR = aggTotal == 0L ? 0.0 : (double) aggEmpty / aggTotal;
            double aggAvgD   = aggTotal == 0L ? 0.0 : (double) aggDepthSum / aggTotal;
            sb.append(String.format(
                    "aggregate.trueAvgDepth=%.3f, emptyRatio=%.4f, nonEmptyRatio=%.4f, subTicks=%d%n",
                    aggAvgD, aggEmptyR, 1.0 - aggEmptyR, aggTotal));
            // Cross-shard imbalance on the two new metrics.
            sb.append(String.format("trueAvgDepth.maxOverMean=%.3f%n", maxOverMean(trueAvgPerQueue)));
            sb.append(String.format("emptyRatio.min=%.4f, max=%.4f%n",
                    minOf(emptyRatioArr), maxOf(emptyRatioArr)));
            sb.append(String.format("nonEmptyRatio.min=%.4f, max=%.4f%n",
                    minOf(nonEmptyRatioArr), maxOf(nonEmptyRatioArr)));
        }

        // ---- Imbalance block ----
        sb.append('\n').append("=== Diagnostics: imbalance ===\n");
        long pMin = Long.MAX_VALUE, pMax = Long.MIN_VALUE;
        double execP99Min = Double.POSITIVE_INFINITY, execP99Max = 0;
        double qwP99Min   = Double.POSITIVE_INFINITY, qwP99Max   = 0;
        boolean haveHist = stats[0].histogram != null;
        for (WorkerStats s : stats) {
            if (s.processed < pMin) pMin = s.processed;
            if (s.processed > pMax) pMax = s.processed;
            if (haveHist && s.processed > 0) {
                double e = s.histogram.p99Execution();
                double q = s.histogram.p99QueueWait();
                if (e < execP99Min) execP99Min = e;
                if (e > execP99Max) execP99Max = e;
                if (q < qwP99Min)   qwP99Min   = q;
                if (q > qwP99Max)   qwP99Max   = q;
            }
        }
        sb.append(String.format("processedCount.maxOverMin=%s%n",
                pMin == 0 ? "inf" : String.format("%.3f", (double) pMax / pMin)));
        if (haveHist) {
            sb.append(String.format("executionP99.maxOverMin=%s%n",
                    execP99Min == 0 ? "inf" : String.format("%.3f", execP99Max / execP99Min)));
            sb.append(String.format("queueWaitP99.maxOverMin=%s%n",
                    qwP99Min == 0 ? "inf" : String.format("%.3f", qwP99Max / qwP99Min)));
        }

        // ---- Time-window block (compact: 3 lines per window) ----
        if (cfg.windowSampling() && !windowProcessed.isEmpty()) {
            sb.append('\n').append("=== Diagnostics: time windows ===\n");
            for (int w = 0; w < windowProcessed.size(); w++) {
                long[] dProc = windowProcessed.get(w);
                int[]  dMx   = windowMaxDepth.get(w);

                long cMin = Long.MAX_VALUE, cMax = Long.MIN_VALUE, cSum = 0;
                int worstWorker = -1;
                for (int i = 0; i < workerCount; i++) {
                    long p = dProc[i];
                    if (p < cMin) { cMin = p; worstWorker = i; }
                    if (p > cMax) { cMax = p; }
                    cSum += p;
                }
                double meanProc = cSum / (double) workerCount;
                double maxOverMin = cMin == 0 ? Double.POSITIVE_INFINITY : (double) cMax / cMin;
                int maxDepthInWindow = 0;
                for (int d : dMx) if (d > maxDepthInWindow) maxDepthInWindow = d;
                double depthImbalance = depthImbalanceForWindow(dMx);

                sb.append(String.format(
                        "window[%d].completedPerWorker.min=%d, mean=%.1f, max=%d, maxOverMin=%s, worstWorker=%d%n",
                        w,
                        cMin == Long.MAX_VALUE ? 0 : cMin,
                        meanProc,
                        cMax == Long.MIN_VALUE ? 0 : cMax,
                        Double.isInfinite(maxOverMin) ? "inf" : String.format("%.3f", maxOverMin),
                        worstWorker));
                sb.append(String.format(
                        "window[%d].maxQueueDepth=%d%n", w, maxDepthInWindow));
                sb.append(String.format(
                        "window[%d].queueDepthImbalance=%.3f%n", w, depthImbalance));
            }
        }

        // ---- Window-correlation block (per-window worst queue + cross-window Pearson r) ----
        if (cfg.windowCorrelation() && !windowProcessed.isEmpty()) {
            appendWindowCorrelation(sb);
        }
    }

    /**
     * Emits the per-window worst-queue snapshot plus the cross-window
     * Pearson correlation summary that connects backlog (queue depth)
     * to service-rate symptoms (execution time, slow-task clustering,
     * completion rate, queue-wait).
     *
     * <p>In SHARED mode {@code queueCount=1} so {@code worstQueue} is
     * always 0; the per-worker columns then describe the aggregate
     * pool. In SHARDED mode they describe the worker that owns the
     * worst-backlog shard in each window.
     */
    private void appendWindowCorrelation(StringBuilder sb) {
        final int nWindows = windowProcessed.size();
        // Per-window vectors used both for the printout AND the cross-window
        // Pearson correlations below.
        double[] vDepth    = new double[nWindows];   // worstQueue.maxQueueDepth
        double[] vExecAvg  = new double[nWindows];   // worker (on worst queue) avg execution ms
        double[] vSlow     = new double[nWindows];   // worker (on worst queue) slow-task delta
        double[] vCompleted= new double[nWindows];   // worker (on worst queue) completed delta
        double[] vQwAvg    = new double[nWindows];   // worker (on worst queue) avg queueWait ms
        int[]    vWorstQ   = new int[nWindows];

        sb.append('\n').append("=== Diagnostics: window correlation ===\n");
        for (int w = 0; w < nWindows; w++) {
            int[]  dMx   = windowMaxDepth.get(w);
            long[] dProc = windowProcessed.get(w);
            long[] dExec = windowExecSumNs.get(w);
            long[] dQw   = windowQwSumNs.get(w);
            long[] dSlow = windowSlowDelta.get(w);
            long[] dMB   = windowMaxBurstDelta.get(w);

            // Worst queue = argmax(maxQueueDepth) — primary indicator of
            // backlog formation. SHARED has only queueId=0.
            int worstQ = 0;
            int worstDepth = dMx[0];
            for (int q = 1; q < queueCount; q++) {
                if (dMx[q] > worstDepth) { worstDepth = dMx[q]; worstQ = q; }
            }

            // Worker associated with the worst queue:
            //   SHARDED: worker[worstQ]   (1:1 mapping)
            //   SHARED : worker[0]        (single aggregate)
            int workerForQueue = (workerCount == queueCount) ? worstQ : 0;
            long p  = dProc[workerForQueue];
            long e  = dExec[workerForQueue];
            long q  = dQw  [workerForQueue];
            long st = dSlow[workerForQueue];
            long mb = dMB  [workerForQueue];
            double execAvgMs = p == 0 ? 0.0 : (e / (double) p) / 1_000_000.0;
            double qwAvgMs   = p == 0 ? 0.0 : (q / (double) p) / 1_000_000.0;

            sb.append(String.format("window[%d].worstQueue=%d%n", w, worstQ));
            sb.append(String.format("window[%d].worstQueue.maxQueueDepth=%d%n", w, worstDepth));
            sb.append(String.format("window[%d].worker.completedDelta=%d%n",  w, p));
            sb.append(String.format("window[%d].worker.execAvgMs=%.3f%n",     w, execAvgMs));
            sb.append(String.format("window[%d].worker.slowTasksDelta=%d%n",  w, st));
            sb.append(String.format("window[%d].worker.maxSlowBurstDelta=%d%n", w, mb));
            sb.append(String.format("window[%d].worker.queueWaitAvgMs=%.3f%n", w, qwAvgMs));

            vDepth[w]     = worstDepth;
            vExecAvg[w]   = execAvgMs;
            vSlow[w]      = st;
            vCompleted[w] = p;
            vQwAvg[w]     = qwAvgMs;
            vWorstQ[w]    = worstQ;
        }

        // Cross-window Pearson correlations: backlog vs. service-rate
        // symptoms. Interpretation (also documented in spec):
        //   queueDepth vs execAvg        > 0  → backlog coincides with slower execution
        //   queueDepth vs slowTasksDelta > 0  → backlog coincides with slow-task clustering
        //   queueDepth vs completedDelta < 0  → backlog coincides with reduced service rate
        //   queueDepth vs queueWaitAvg   > 0  → backlog matches observed queue wait
        sb.append('\n');
        sb.append(String.format("correlation.queueDepth_vs_execAvg=%s%n",
                fmtCorr(pearson(vDepth, vExecAvg))));
        sb.append(String.format("correlation.queueDepth_vs_slowTasksDelta=%s%n",
                fmtCorr(pearson(vDepth, vSlow))));
        sb.append(String.format("correlation.queueDepth_vs_completedDelta=%s%n",
                fmtCorr(pearson(vDepth, vCompleted))));
        sb.append(String.format("correlation.queueDepth_vs_queueWaitAvg=%s%n",
                fmtCorr(pearson(vDepth, vQwAvg))));
    }

    /** Pearson product-moment correlation; returns NaN for degenerate inputs. */
    private static double pearson(double[] xs, double[] ys) {
        if (xs.length < 2 || xs.length != ys.length) return Double.NaN;
        double n = xs.length;
        double sx = 0, sy = 0;
        for (int i = 0; i < xs.length; i++) { sx += xs[i]; sy += ys[i]; }
        double mx = sx / n, my = sy / n;
        double num = 0, dx2 = 0, dy2 = 0;
        for (int i = 0; i < xs.length; i++) {
            double dx = xs[i] - mx, dy = ys[i] - my;
            num += dx * dy;
            dx2 += dx * dx;
            dy2 += dy * dy;
        }
        if (dx2 == 0.0 || dy2 == 0.0) return Double.NaN;
        return num / Math.sqrt(dx2 * dy2);
    }

    private static String fmtCorr(double r) {
        return Double.isNaN(r) ? "nan" : String.format("%.3f", r);
    }

    /* ---------------- helpers ---------------- */

    private static double maxOverMean(double[] vals) {
        if (vals.length == 0) return 0.0;
        double sum = 0, mx = 0;
        for (double v : vals) { sum += v; if (v > mx) mx = v; }
        double mean = sum / vals.length;
        return mean == 0 ? 0.0 : mx / mean;
    }

    private static double minOf(double[] vals) {
        if (vals.length == 0) return 0.0;
        double m = Double.POSITIVE_INFINITY;
        for (double v : vals) if (v < m) m = v;
        return m;
    }

    private static double maxOf(double[] vals) {
        if (vals.length == 0) return 0.0;
        double m = Double.NEGATIVE_INFINITY;
        for (double v : vals) if (v > m) m = v;
        return m;
    }

    private static double[] toDoubles(int[] a) {
        double[] r = new double[a.length];
        for (int i = 0; i < a.length; i++) r[i] = a[i];
        return r;
    }

    private static int percentileInt(int[] data, int pct) {
        if (data.length == 0) return 0;
        int[] copy = data.clone();
        Arrays.sort(copy);
        int idx = (int) Math.ceil(pct / 100.0 * copy.length) - 1;
        if (idx < 0) idx = 0;
        if (idx >= copy.length) idx = copy.length - 1;
        return copy[idx];
    }

    private static double depthImbalanceForWindow(int[] depths) {
        if (depths.length == 0) return 0.0;
        long sum = 0; int mx = 0;
        for (int d : depths) { sum += d; if (d > mx) mx = d; }
        double mean = sum / (double) depths.length;
        return mean == 0 ? 0.0 : mx / mean;
    }
}

