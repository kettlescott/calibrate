package com.scott;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sharded Executor — per-worker queue implementation.
 *
 * <p>Each of the {@code workerCount} worker threads owns a dedicated
 * {@link LinkedBlockingQueue}.  Tasks are routed to a shard by
 * {@code Hashing.shardOf(task.routingKey(), workerCount, routing)},
 * where {@link ShardedRoutingConfig} selects the concrete function
 * (default: {@code MODULO} on {@code Long.hashCode} with floor-mod,
 * uniform for dense sequential keys). Workers consume <em>only</em>
 * their own queue — there is no work stealing.
 *
 * <p>This is the direct counterpart of {@link SharedExecutor} for
 * queue-contention benchmarks: same {@link Task}, same 4-stage timing
 * model (created / enqueued / start / finish — see {@link Task}), same
 * {@link LatencyRecorder} — the <em>only</em> structural difference is
 * the queue topology.
 *
 * <h3>Why this design?</h3>
 * <p>A per-worker queue eliminates the single shared-queue lock as a
 * contention point.  With uniform hash-based routing and identical
 * workloads the load is well-balanced, making this the cleanest
 * possible baseline for comparing shared-queue vs.&nbsp;sharded-queue
 * tail latency without confounding variables (no hash skew, no
 * stealing overhead, no adaptive routing).
 *
 * <h3>Trade-offs vs. SharedExecutor (single shared queue)</h3>
 * <table>
 *   <tr><th>Aspect</th><th>SharedExecutor</th><th>ShardedExecutor</th></tr>
 *   <tr><td>Queue contention</td>
 *       <td>All workers compete on one queue lock</td>
 *       <td>Each worker has its own queue — zero cross-queue contention</td></tr>
 *   <tr><td>Load balancing</td>
 *       <td>Automatic — idle worker steals from the shared queue</td>
 *       <td>Static — depends on routing hash uniformity; a slow shard
 *           blocks while others sit idle</td></tr>
 *   <tr><td>Cache locality</td>
 *       <td>Poor — queue head is a contended cache line</td>
 *       <td>Better — each queue head is accessed by one consumer</td></tr>
 *   <tr><td>Head-of-line blocking</td>
 *       <td>One slow task blocks the queue head for all workers</td>
 *       <td>One slow task only blocks its own shard</td></tr>
 *   <tr><td>Tail latency</td>
 *       <td>Can spike under contention</td>
 *       <td>More predictable when load is balanced</td></tr>
 *   <tr><td>Complexity</td>
 *       <td>Trivial — JDK ThreadPoolExecutor</td>
 *       <td>Slightly more — hand-spun worker loop + routing</td></tr>
 * </table>
 *
 * <h3>Fairness note — unbounded queues</h3>
 * <p>{@link SharedExecutor} uses an unbounded {@link LinkedBlockingQueue};
 * this executor uses the same queue type per worker so the <em>only</em>
 * experimental variable is the queue topology (1 shared vs.&nbsp;N
 * per-worker).  Bounded queues would introduce backpressure on
 * {@code submit()}, adding a confounding variable to latency
 * measurements.</p>
 *
 * <h3>GC note</h3>
 * <ul>
 *   <li>{@link LinkedBlockingQueue} allocates a {@code Node} per enqueue
 *       (unlike {@code ArrayBlockingQueue}'s pre-allocated array), but
 *       matches the allocation behaviour of {@link SharedExecutor}.</li>
 *   <li>No {@link Runnable} wrappers, no {@code FutureTask}, no boxing.</li>
 *   <li>{@link Task#run()} handles all timing internally — the worker
 *       adds zero measurement overhead beyond the queue dequeue.</li>
 * </ul>
 *
 * <h3>Known limitations (first version)</h3>
 * <ul>
 *   <li>No work stealing — a slow shard blocks while others sit idle.</li>
 *   <li>No NUMA-aware placement.</li>
 *   <li>Hash-based routing assumes uniform task cost; skewed workloads
 *       will cause imbalance.</li>
 * </ul>
 */
public final class ShardedExecutor implements BenchmarkExecutor {

    private final int workerCount;
    private final LinkedBlockingQueue<Task>[] queues;
    private final Thread[] workerThreads;
    private final ShardedWorker[] workers;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /** Routing policy (legacy {@code MODULO} by default). */
    private final ShardedRoutingConfig routing;

    // ---- task tracking (DEBUG-only) ----
    // When BenchmarkFlags.DEBUG is true, every submitted Task reference is
    // recorded for diagnostic use via getTasks().  When false (default), the
    // list is null and submit() performs zero tracking — no Node allocation,
    // no CAS, no GC pressure on the hot path.
    private final ArrayList<Task> taskList;

    // Simple counters — written only by the (single) submitter thread,
    // so plain int is safe.  Read after shutdown.
    private int totalSubmitCount;
    private int measurementSubmitCount;

    /* ================================================================
     *  Constructors
     * ================================================================ */

    /** Creates a sharded executor with default routing and no diagnostics hooks. */
    public ShardedExecutor(int workerCount) {
        this(workerCount, null, ShardedRoutingConfig.defaults(), null);
    }

    /**
     * Full constructor. {@code workerStats} may be null when diagnostics
     * are disabled (default). When non-null, the array length must equal
     * {@code workerCount} — entry {@code [i]} is forwarded to worker
     * {@code i} and updated by that worker on the hot path.
     */
    public ShardedExecutor(int workerCount, WorkerStats[] workerStats) {
        this(workerCount, workerStats, ShardedRoutingConfig.defaults(), null);
    }

    /**
     * Routing-aware constructor. {@code routing} controls how {@code taskId}
     * is mapped to a shard at submit time (see {@link ShardedRoutingConfig}).
     */
    /** Full constructor with optional routing and diagnostics hooks. */
    @SuppressWarnings("unchecked")
    public ShardedExecutor(int workerCount,
                           WorkerStats[] workerStats,
                           ShardedRoutingConfig routing,
                           WorkerBusyIdleTracker busyIdle) {
        this.workerCount    = workerCount;
        this.routing        = routing == null ? ShardedRoutingConfig.defaults() : routing;
        this.queues         = new LinkedBlockingQueue[workerCount];
        this.workerThreads  = new Thread[workerCount];
        this.workers        = new ShardedWorker[workerCount];
        this.taskList       = BenchmarkFlags.DEBUG ? new ArrayList<>() : null;

        if (workerStats != null && workerStats.length != workerCount) {
            throw new IllegalArgumentException(
                    "workerStats.length (" + workerStats.length
                            + ") must equal workerCount (" + workerCount + ")");
        }

        for (int i = 0; i < workerCount; i++) {
            WorkerStats statsForWorker = workerStats == null ? null : workerStats[i];
            LinkedBlockingQueue<Task> preAlloc = new LinkedBlockingQueue<>();
            queues[i] = preAlloc;

            ShardedWorker worker = new ShardedWorker(
                    i, preAlloc, shutdown, statsForWorker, busyIdle);

            workers[i]       = worker;
            workerThreads[i] = new Thread(worker, "ShardedWorker-" + i);
            workerThreads[i].setDaemon(false);
        }

        for (Thread t : workerThreads) {
            t.start();
        }

    }

    /**
     * When DEBUG is {@code true}, each task is also added to an internal
     * list for diagnostic use via {@link #getTasks()}.
     */
    @Override
    public void submit(Task task) throws InterruptedException {
        if (BenchmarkFlags.DEBUG && taskList != null) {
            taskList.add(task);
        }
        totalSubmitCount++;
        if (task.isMeasurement()) {
            measurementSubmitCount++;
        }
        int shard = Hashing.shardOf(task.routingKey(), workerCount, routing);
        queues[shard].offer(task);
    }


    /* ================================================================
     *  Lifecycle
     * ================================================================ */

    /**
     * Signals all workers to finish draining their queues and exit.
     *
     * <p>Sets the shared shutdown flag and interrupts every worker thread
     * so that threads blocked in {@code take()} wake up immediately.
     */
    @Override
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }
        for (Thread t : workerThreads) {
            t.interrupt();
        }
    }

    /**
     * Blocks until every worker thread has terminated or the timeout
     * expires, whichever comes first.
     *
     * <p>Uses {@link Thread#join(long, int)} with both millisecond and
     * nanosecond components so that sub-millisecond remainders are not
     * truncated to zero (which would make {@code join} wait forever).
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        for (Thread t : workerThreads) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return !anyAlive();
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
            int  nanos  = (int) (remainingNanos % 1_000_000);
            t.join(millis, nanos);
            if (t.isAlive()) return false;
        }
        return true;
    }

    /** Returns {@code true} if any worker thread is still alive. */
    private boolean anyAlive() {
        for (Thread t : workerThreads) {
            if (t.isAlive()) return true;
        }
        return false;
    }

    /* ================================================================
     *  Observation (for diagnostics / tests)
     * ================================================================ */

    /** Returns the number of worker threads (same semantics as SharedExecutor.getPoolSize()). */
    public int getPoolSize() {
        return workerCount;
    }

    /** Alias for {@link #getPoolSize()} — same value. */
    public int getWorkerCount() {
        return workerCount;
    }

    /** Returns the active routing policy (never null). */
    public ShardedRoutingConfig routing() {
        return routing;
    }


    /**
     * Returns an unmodifiable snapshot of all submitted {@link Task}s.
     *
     * <p>Only populated when {@link BenchmarkFlags#DEBUG} is {@code true}.
     * Returns an empty list when task tracking is disabled (default) to
     * avoid allocation overhead on the submit hot path.
     */
    public List<Task> getTasks() {
        if (taskList == null) return Collections.emptyList();
        return Collections.unmodifiableList(new ArrayList<>(taskList));
    }

    /** Returns the current depth of a single shard queue. */
    public int getQueueSize(int shard) {
        return queues[shard].size();
    }

    /** Returns the sum of all shard queue depths. */
    public int getTotalQueueSize() {
        int total = 0;
        for (LinkedBlockingQueue<Task> q : queues) {
            total += q.size();
        }
        return total;
    }

    /* ================================================================
     *  Per-queue task distribution (load-imbalance diagnostics)
     * ================================================================ */

    /**
     * Returns the total number of tasks processed by a specific worker
     * (warmup + measurement).
     */
    public long getProcessedCount(int workerIndex) {
        return workers[workerIndex].processedCount();
    }

    /**
     * Returns a defensive copy of per-worker total processed counts
     * (warmup + measurement).
     */
    public long[] getProcessedCounts() {
        long[] counts = new long[workerCount];
        for (int i = 0; i < workerCount; i++) {
            counts[i] = workers[i].processedCount();
        }
        return counts;
    }

    /**
     * Returns a defensive copy of per-worker <em>measurement-only</em>
     * processed counts.  Only tasks where {@link Task#isMeasurement()}
     * is {@code true} are included.
     */
    public long[] getMeasurementProcessedCounts() {
        long[] counts = new long[workerCount];
        for (int i = 0; i < workerCount; i++) {
            counts[i] = workers[i].measurementProcessedCount();
        }
        return counts;
    }

    /**
     * Prints a formatted per-queue task distribution summary showing
     * <em>measurement-phase only</em> counts.  Call after shutdown +
     * awaitTermination so all workers have finished and counters are final.
     */
    public void printQueueDistribution() {
        long[] allCounts  = getProcessedCounts();
        long[] measCounts = getMeasurementProcessedCounts();

        long allTotal  = 0;
        long measTotal = 0;
        long measMin   = Long.MAX_VALUE;
        long measMax   = Long.MIN_VALUE;
        for (int i = 0; i < workerCount; i++) {
            allTotal  += allCounts[i];
            measTotal += measCounts[i];
            if (measCounts[i] < measMin) measMin = measCounts[i];
            if (measCounts[i] > measMax) measMax = measCounts[i];
        }
        double measAvg = (workerCount > 0) ? (double) measTotal / workerCount : 0.0;

        System.out.println();
        System.out.println("=== Per-Queue Task Distribution (measurement only) ===");
        for (int i = 0; i < workerCount; i++) {
            double pct = (measTotal > 0) ? measCounts[i] * 100.0 / measTotal : 0.0;
            System.out.printf("  queue[%d] processed: %7d  (%5.1f%%)", i, measCounts[i], pct);
            if (measCounts[i] == measMax && measMax > measAvg * 1.1) {
                System.out.print("   <-- max");
            } else if (measCounts[i] == measMin && measMin < measAvg * 0.9) {
                System.out.print("   <-- min");
            }
            System.out.println();
        }
        System.out.printf("  total (measurement) : %d%n", measTotal);
        System.out.printf("  total (all phases)  : %d%n", allTotal);
        System.out.printf("  warmup (implied)    : %d%n", allTotal - measTotal);
        System.out.printf("  min     : %d%n", measMin);
        System.out.printf("  max     : %d%n", measMax);
        System.out.printf("  average : %.1f%n", measAvg);
        double maxMeanLoadRatio = (measAvg > 0) ? measMax / measAvg : 0.0;
        System.out.printf("  max/mean load ratio (observed): %.2f%n", maxMeanLoadRatio);
    }
}

