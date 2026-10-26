package com.scott;

import java.util.concurrent.TimeUnit;

/**
 * Common interface for executor implementations under benchmark.
 *
 * <p>Both {@link SharedExecutor} (single shared queue) and
 * {@link ShardedExecutor} (per-worker queues) implement this interface
 * so that the benchmark runner can test either one with identical task
 * generation, timing semantics, and latency recording.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>{@link #submit(Task)} enqueues a pre-built {@link Task} for execution.
 *       The task's {@link Task#run()} method is the <em>only</em> code that
 *       the executor must invoke — it already records start/finish timestamps,
 *       executes the workload, and counts down the batch latch.</li>
 *   <li>{@link #shutdown()} initiates an orderly shutdown: no new tasks are
 *       accepted, but already-enqueued tasks are drained and completed.</li>
 *   <li>{@link #awaitTermination(long, TimeUnit)} blocks until all worker
 *       threads have finished or the timeout expires.</li>
 * </ul>
 */
public interface BenchmarkExecutor {

    /**
     * Submits a pre-built task for execution.
     *
     * <p>Implementations may block if the underlying queue is full
     * (e.g.&nbsp;bounded queue variants). Task timing (start/finish) is
     * recorded inside {@link Task#run()} by the worker thread.
     *
     * @param task the benchmark task — {@code createdNanos} (and
     *             typically {@code enqueuedNanos}, set by the
     *             {@link Dispatcher}) are already populated
     * @throws InterruptedException if the calling thread is interrupted
     *                              while waiting for queue space
     */
    void submit(Task task) throws InterruptedException;

    /**
     * Initiates an orderly shutdown.  Already-enqueued tasks will still
     * be executed; subsequent {@link #submit} calls may be rejected.
     */
    void shutdown();

    /**
     * Blocks until all worker threads have terminated or the timeout
     * expires, whichever comes first.
     *
     * @return {@code true} if the executor terminated within the timeout
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}

