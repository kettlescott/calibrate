package com.scott;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker thread for {@link ShardedExecutor}.
 *
 * <p>Each worker owns a dedicated {@link LinkedBlockingQueue} and is the
 * <em>sole consumer</em> of that queue.  The hot loop is deliberately
 * minimal: dequeue → {@link Task#run()} → repeat.  {@code Task.run()}
 * already handles all timing bookkeeping (recordStart, execute workload,
 * recordFinish, countDown latch), so the worker adds no measurement
 * overhead beyond the queue dequeue itself.
 *
 * <h3>Shutdown protocol</h3>
 * <ol>
 *   <li>The executor sets {@code shutdown = true} and interrupts this thread.</li>
 *   <li>If blocked in {@code take()}, {@link InterruptedException} is caught.</li>
 *   <li>Any remaining tasks in the local queue are drained and executed.</li>
 *   <li>When {@code shutdown == true && localQueue.isEmpty()}, the worker exits.</li>
 * </ol>
 *
 * <h3>GC notes</h3>
 * <ul>
 *   <li>No objects are allocated on the hot path — no wrappers, no futures,
 *       no lambda captures.</li>
 *   <li>No logging or string formatting in the loop.</li>
 * </ul>
 */
final class ShardedWorker implements Runnable {

    private final int workerId;
    /** Pre-created per-worker queue passed by the executor. */
    private final LinkedBlockingQueue<Task> localQueue;
    private final AtomicBoolean shutdown;

    /**
     * Optional per-worker diagnostics sink. Null when diagnostics are
     * disabled (default) — hot-path stays allocation-free and the JIT
     * eliminates the {@code stats != null} branch.
     */
    private final WorkerStats stats;

    /** Optional per-worker busy/idle tracker; null when diagnostics disabled. */
    private final WorkerBusyIdleTracker busyIdle;

    /**
     * Number of tasks this worker has executed.  Only written by the
     * owning worker thread and read after the thread has terminated,
     * so no synchronization is needed (plain {@code long} is fine).
     */
    private long processedCount;

    /**
     * Number of <em>measurement-phase</em> tasks this worker has executed.
     * Incremented only when {@link Task#isMeasurement()} is {@code true}.
     * Same thread-safety rationale as {@code processedCount}.
     */
    private long measurementProcessedCount;

    /** Creates a worker with optional diagnostics hooks. */
    ShardedWorker(int workerId,
                  LinkedBlockingQueue<Task> localQueue,
                  AtomicBoolean shutdown,
                  WorkerStats stats,
                  WorkerBusyIdleTracker busyIdle) {
        this.workerId          = workerId;
        this.localQueue        = localQueue;
        this.shutdown          = shutdown;
        this.stats             = stats;
        this.busyIdle          = busyIdle;
    }

    @Override
    public void run() {
        final LinkedBlockingQueue<Task> effectiveQueue = localQueue;


        // ---- Main task loop — kept minimal to match src_before behavior.
        //      All setup logic is in helper methods to keep this method small
        //      for optimal JIT compilation and inlining. ----
        try {
            while (true) {
                if (shutdown.get() && effectiveQueue.isEmpty()) {
                    return;
                }

                final Task task;
                try {
                    task = effectiveQueue.take();
                } catch (InterruptedException e) {
                    if (shutdown.get() && effectiveQueue.isEmpty()) {
                        return;
                    }
                    continue;
                }

                // Identical to src_before hot path: single condition, no extra work.
                if (stats != null) {
                    task.runWithBeforeComplete(stats);
                } else {
                    task.run();
                }


                processedCount++;
                if (task.isMeasurement()) {
                    measurementProcessedCount++;
                }
            }
        } finally {
            // no-op
        }
    }

    /**
     * Returns the number of tasks this worker has executed.
     *
     * <p>Only meaningful after the worker thread has terminated —
     * the caller must {@code join()} the thread first to establish
     * a happens-before relationship.
     */
    long processedCount() {
        return processedCount;
    }

    /**
     * Returns the number of measurement-phase tasks this worker has executed.
     * Only meaningful after the worker thread has terminated.
     */
    long measurementProcessedCount() {
        return measurementProcessedCount;
    }

    int workerId() {
        return workerId;
    }

}
