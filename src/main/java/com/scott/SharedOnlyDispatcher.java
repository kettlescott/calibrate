package com.scott;

import java.util.concurrent.TimeUnit;

/**
 * Dispatcher that routes tasks to one or more {@link SharedExecutor}
 * sub-pools (Experiment 1: shared-queue contention).
 *
 * <h3>Controlled variable</h3>
 * The only experimental variable is <em>workers sharing a queue</em>.
 * Total worker count and total offered workload are unchanged. For
 * 32 workers:
 * <ul>
 *   <li>{@code sharedQueueCount=1} → one queue, 32 workers/queue</li>
 *   <li>{@code sharedQueueCount=2} → two queues, 16 workers/queue</li>
 *   <li>{@code sharedQueueCount=4} → four queues, 8 workers/queue</li>
 * </ul>
 *
 * <h3>Worker-id partitioning</h3>
 * Sub-pool {@code i} owns global worker IDs
 * {@code [offset_i, offset_i + size_i)} where sizes are as-even-as-possible
 * (extra workers go to the first {@code workerCount % K} sub-pools). This
 * guarantees globally unique worker IDs across sub-pools — required for
 * {@link WorkerBusyIdleTracker} slots.
 *
 * <h3>Routing (deterministic, no atomics on submit path)</h3>
 * Sub-pool selection is {@code queueIndex = floorMod(task.taskId(), K)}.
 * With dense sequential {@code taskId}s produced by the benchmark runner
 * this yields perfectly balanced round-robin without any shared
 * {@link java.util.concurrent.atomic.AtomicLong} — avoiding a global
 * contention point that would confound the measurement.
 *
 * <h3>{@code sharedQueueCount == 1}</h3>
 * The dispatcher holds a single-element {@code SharedExecutor[]} and
 * takes the direct {@code executors[0].submit(task)} path. There are
 * <em>no</em> extra atomics or counters on the submit path — the only
 * differences from the legacy single-executor implementation are
 * (a) an array indirection {@code executors[0]} and (b) a
 * {@code Math.floorMod} call that is skipped by the fast-path branch.
 * It is <em>not</em> claimed byte-identical.
 */
public final class SharedOnlyDispatcher implements Dispatcher {

    private final SharedExecutor[] executors;

    public SharedOnlyDispatcher(int workerCount) {
        this(workerCount, null, null, 1);
    }

    public SharedOnlyDispatcher(int workerCount, WorkerStats stats) {
        this(workerCount, stats, null, 1);
    }

    public SharedOnlyDispatcher(int workerCount, WorkerStats stats,
                                WorkerBusyIdleTracker busyIdle) {
        this(workerCount, stats, busyIdle, 1);
    }

    /** Full constructor supporting Experiment 1 (sharedQueueCount >= 1). */
    public SharedOnlyDispatcher(int workerCount, WorkerStats stats,
                                WorkerBusyIdleTracker busyIdle, int sharedQueueCount) {
        if (sharedQueueCount <= 0) {
            throw new IllegalArgumentException("sharedQueueCount must be > 0");
        }
        if (sharedQueueCount > workerCount) {
            throw new IllegalArgumentException(
                    "sharedQueueCount (" + sharedQueueCount + ") must be <= workerCount (" + workerCount + ")");
        }
        this.executors = new SharedExecutor[sharedQueueCount];
        int base = workerCount / sharedQueueCount;
        int extra = workerCount % sharedQueueCount;
        int offset = 0;
        for (int i = 0; i < sharedQueueCount; i++) {
            int size = base + (i < extra ? 1 : 0);
            // workerIdOffset guarantees globally-unique worker IDs across sub-pools.
            executors[i] = new SharedExecutor(size, stats, busyIdle, offset);
            offset += size;
        }
    }

    @Override
    public void submit(Task task) throws InterruptedException {
        task.markEnqueued();
        // Fast path: single queue, no routing, no floorMod.
        if (executors.length == 1) {
            executors[0].submit(task);
            return;
        }
        // Deterministic, balanced routing with zero atomics on the submit
        // path. taskId is dense/sequential in the benchmark runner, so
        // floorMod yields perfect round-robin across sub-pools.
        int idx = (int) Math.floorMod(task.taskId(), executors.length);
        executors[idx].submit(task);
    }

    @Override
    public void shutdown() {
        for (SharedExecutor e : executors) e.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (SharedExecutor e : executors) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return false;
            if (!e.awaitTermination(remaining, TimeUnit.NANOSECONDS)) return false;
        }
        return true;
    }

    @Override
    public String label() {
        return executors.length == 1 ? "SharedOnly" : ("SharedOnly[q=" + executors.length + "]");
    }

    @Override
    public int totalQueueSize() {
        int sum = 0;
        for (SharedExecutor e : executors) sum += e.getQueueSize();
        return sum;
    }

    /** Legacy accessor: first sub-pool. */
    public SharedExecutor executor() { return executors[0]; }

    /** All shared sub-pools, in stable order. */
    public SharedExecutor[] executors() { return executors; }

    /**
     * Per-sub-pool measurement-submit counts. Sourced from each
     * sub-pool's existing counter — no new hot-path counter, no
     * atomic on the submit path.
     */
    public long[] measurementSubmitCounts() {
        long[] out = new long[executors.length];
        for (int i = 0; i < out.length; i++) out[i] = executors[i].getMeasurementSubmitCount();
        return out;
    }

    /** Per-sub-pool completed-task counts (from the TPE). */
    public long[] completedCounts() {
        long[] out = new long[executors.length];
        for (int i = 0; i < out.length; i++) out[i] = executors[i].getCompletedTaskCount();
        return out;
    }

    /** Per-sub-pool current queue depths. */
    public int[] queueDepths() {
        int[] out = new int[executors.length];
        for (int i = 0; i < out.length; i++) out[i] = executors[i].getQueueSize();
        return out;
    }
}

