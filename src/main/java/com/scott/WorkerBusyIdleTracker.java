package com.scott;

/**
 * Per-worker busy vs. idle (waiting-for-work) time tracker.
 *
 * <h3>What is measured</h3>
 * <ul>
 *   <li>{@code busyNs[w]}  — sum of wall-clock time worker {@code w}
 *       spent inside {@code task.run()} (executing user workload).</li>
 *   <li>{@code idleNs[w]}  — sum of wall-clock time worker {@code w}
 *       spent between the finish of one task and the start of the
 *       next (i.e. blocked in {@code take()} for sharded, or between
 *       {@code afterExecute} and the next {@code beforeExecute} for
 *       the shared {@link java.util.concurrent.ThreadPoolExecutor}).</li>
 *   <li>{@code tasks[w]}   — number of tasks the worker executed.</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 * Each array slot {@code [w]} is written by exactly one worker thread,
 * read only after {@link Dispatcher#awaitTermination}. Plain long stores
 * are safe (happens-before via join). No atomics, no locks, no volatiles
 * — cost per task is 2× {@code System.nanoTime()} + a handful of long
 * ops (~50 ns total on modern x86, negligible against a 50 µs task).
 *
 * <p>Not created / not touched unless the diagnostics block is enabled.
 */
public final class WorkerBusyIdleTracker {

    private final int workerCount;
    private final long[] busyNs;
    private final long[] idleNs;
    private final long[] tasks;
    /** Timestamp of the previous task's finish (or worker-start) per worker. */
    private final long[] lastFinishNs;
    /** Task-start timestamp captured in beforeTask(); consumed by afterTask(). */
    private final long[] taskStartNs;

    public WorkerBusyIdleTracker(int workerCount) {
        this.workerCount = workerCount;
        this.busyNs       = new long[workerCount];
        this.idleNs       = new long[workerCount];
        this.tasks        = new long[workerCount];
        this.lastFinishNs = new long[workerCount];
        this.taskStartNs  = new long[workerCount];
    }

    /** Optional: call once when the worker thread comes online so the
     *  first idle interval (thread-start → first task) is accounted. */
    public void workerStarted(int w, long nowNanos) {
        lastFinishNs[w] = nowNanos;
    }

    /** Called immediately BEFORE {@code task.run()}. Accumulates idle
     *  interval since the previous finish. */
    public void beforeTask(int w, long nowNanos) {
        long lf = lastFinishNs[w];
        if (lf != 0L) {
            long d = nowNanos - lf;
            if (d > 0L) idleNs[w] += d;
        }
        taskStartNs[w] = nowNanos;
    }

    /** Called immediately AFTER {@code task.run()}. Accumulates busy
     *  interval and increments task count. */
    public void afterTask(int w, long nowNanos) {
        long ts = taskStartNs[w];
        if (ts != 0L) {
            long d = nowNanos - ts;
            if (d > 0L) busyNs[w] += d;
        }
        tasks[w]++;
        lastFinishNs[w] = nowNanos;
    }

    public int workerCount()           { return workerCount; }
    public long busyNs(int w)          { return busyNs[w]; }
    public long idleNs(int w)          { return idleNs[w]; }
    public long tasks(int w)           { return tasks[w]; }

    /**
     * Appends a per-worker + aggregate busy/idle summary block to
     * {@code out}. Called after workers have terminated.
     */
    public void appendSummary(StringBuilder out) {
        out.append('\n').append("=== Diagnostics: worker busy / idle ===").append('\n');
        out.append("  (busyNs   = time inside task.run();  "
                +   "idleNs = time between finish and next start)\n");
        out.append(String.format(
                "  %-8s %14s %14s %14s %8s %8s%n",
                "worker", "busyMs", "idleMs", "tasks", "busy%", "idle%"));

        double[] busyRatio = new double[workerCount];
        double[] idleRatio = new double[workerCount];
        long[]   taskArr   = new long[workerCount];
        long totalBusy = 0L, totalIdle = 0L, totalTasks = 0L;

        for (int i = 0; i < workerCount; i++) {
            long b = busyNs[i], id = idleNs[i], tk = tasks[i];
            totalBusy += b; totalIdle += id; totalTasks += tk;
            long span = b + id;
            double br = span == 0 ? 0.0 : (double) b  / span;
            double ir = span == 0 ? 0.0 : (double) id / span;
            busyRatio[i] = br;
            idleRatio[i] = ir;
            taskArr[i]   = tk;
            out.append(String.format(
                    "  %-8d %14.3f %14.3f %14d %7.2f%% %7.2f%%%n",
                    i, b / 1e6, id / 1e6, tk, br * 100.0, ir * 100.0));
        }

        // Aggregate stats: mean / min / max / stddev / CV for busy%,
        // idle%, tasks. CV = stddev / mean; useful for "how balanced".
        Agg br = agg(busyRatio);
        Agg ir = agg(idleRatio);
        Agg tk = aggLong(taskArr);

        out.append('\n');
        out.append(String.format("  aggregate.workers=%d%n", workerCount));
        out.append(String.format("  aggregate.totalBusyMs=%.3f, totalIdleMs=%.3f, totalTasks=%d%n",
                totalBusy / 1e6, totalIdle / 1e6, totalTasks));
        double aggBusy = (totalBusy + totalIdle) == 0
                ? 0.0 : (double) totalBusy / (totalBusy + totalIdle);
        out.append(String.format("  aggregate.busyRatio=%.4f  (sum(busy)/sum(busy+idle))%n", aggBusy));

        out.append(String.format("  busyRatio.mean=%.4f, min=%.4f, max=%.4f, stddev=%.4f, cv=%.4f%n",
                br.mean, br.min, br.max, br.stddev, br.cv()));
        out.append(String.format("  idleRatio.mean=%.4f, min=%.4f, max=%.4f, stddev=%.4f, cv=%.4f%n",
                ir.mean, ir.min, ir.max, ir.stddev, ir.cv()));
        out.append(String.format("  tasksPerWorker.mean=%.2f, min=%d, max=%d, stddev=%.2f, cv=%.4f%n",
                tk.mean, (long) tk.min, (long) tk.max, tk.stddev, tk.cv()));
    }

    /* ---------- tiny local stats helper (no allocations elsewhere) ---------- */

    private static final class Agg {
        final double mean, min, max, stddev;
        Agg(double mean, double min, double max, double stddev) {
            this.mean = mean; this.min = min; this.max = max; this.stddev = stddev;
        }
        double cv() { return mean == 0.0 ? 0.0 : stddev / mean; }
    }

    private static Agg agg(double[] xs) {
        if (xs.length == 0) return new Agg(0, 0, 0, 0);
        double sum = 0, mn = Double.POSITIVE_INFINITY, mx = Double.NEGATIVE_INFINITY;
        for (double v : xs) { sum += v; if (v < mn) mn = v; if (v > mx) mx = v; }
        double mean = sum / xs.length;
        double sq = 0;
        for (double v : xs) { double d = v - mean; sq += d * d; }
        double sd = Math.sqrt(sq / xs.length);
        return new Agg(mean, mn, mx, sd);
    }

    private static Agg aggLong(long[] xs) {
        if (xs.length == 0) return new Agg(0, 0, 0, 0);
        long sum = 0, mn = Long.MAX_VALUE, mx = Long.MIN_VALUE;
        for (long v : xs) { sum += v; if (v < mn) mn = v; if (v > mx) mx = v; }
        double mean = (double) sum / xs.length;
        double sq = 0;
        for (long v : xs) { double d = v - mean; sq += d * d; }
        double sd = Math.sqrt(sq / xs.length);
        return new Agg(mean, mn, mx, sd);
    }
}

