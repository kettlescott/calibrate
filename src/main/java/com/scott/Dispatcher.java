package com.scott;

import java.util.concurrent.TimeUnit;

/**
 * Routing policy that accepts benchmark {@link Task}s and forwards them
 * to one or more execution backends ({@link SharedExecutor},
 * {@link ShardedExecutor}, or both).
 *
 * <p>The dispatcher is the single entry point used by
 * {@link BenchmarkMain} — swapping the dispatcher implementation is the
 * only change needed to compare different routing policies under the
 * same workload.
 *
 * <h3>Implementations</h3>
 * <ul>
 *   <li>{@link SharedOnlyDispatcher}  — all tasks → shared queue</li>
 *   <li>{@link ShardedOnlyDispatcher} — all tasks → sharded queues</li>
 *   <li>{@link HybridDispatcher}      — route by {@link WorkloadKind}</li>
 * </ul>
 */
public interface Dispatcher {

    /**
     * Submits a task for execution via this dispatcher's routing policy.
     *
     * <p>Implementations are expected to call {@link Task#markEnqueued()}
     * on the task just before handing it to the backing executor so that
     * {@link Task#queueWaitTimeNanos()} measures time actually spent in
     * the queue (and not the dispatcher's own work).
     *
     * @param task the benchmark task — its {@code createdNanos} must
     *             already have been set by the generator
     * @throws InterruptedException if the calling thread is interrupted
     *                              while waiting for queue space
     */
    void submit(Task task) throws InterruptedException;

    /**
     * Initiates an orderly shutdown of all backing executors.
     * Already-submitted tasks will still be executed.
     */
    void shutdown();

    /**
     * Blocks until all backing executors have terminated or the timeout
     * expires, whichever comes first.
     *
     * @return {@code true} if all executors terminated within the timeout
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns a short human-readable label for this dispatcher
     * (e.g.&nbsp;{@code "SharedOnly"}, {@code "Hybrid"}).
     */
    String label();

    /**
     * Returns the current total queue depth across <em>all</em> backing
     * queues (sum of every shard / shared queue managed by this
     * dispatcher). Used by the queue-depth sampler.
     *
     * <p>Returns {@code -1} when the implementation cannot expose this
     * cheaply; callers should treat {@code -1} samples as "unknown" and
     * skip them in averages.
     */
    default int totalQueueSize() { return -1; }
}

