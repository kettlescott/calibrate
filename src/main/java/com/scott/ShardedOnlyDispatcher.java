package com.scott;

import java.util.concurrent.TimeUnit;

/**
 * Dispatcher that routes every task to a {@link ShardedExecutor}
 * (per-worker queues, hash-based routing).
 */
public final class ShardedOnlyDispatcher implements Dispatcher {

    private final ShardedExecutor executor;

    public ShardedOnlyDispatcher(int workerCount) {
        this(workerCount, null);
    }

    /**
     * Diagnostics-aware constructor. {@code workerStats} may be null
     * (default) — when non-null, each entry is wired to one
     * {@link ShardedWorker} so per-worker tail statistics can be
     * collected without any cross-thread synchronisation.
     */
    public ShardedOnlyDispatcher(int workerCount, WorkerStats[] workerStats) {
        this(workerCount, workerStats, ShardedRoutingConfig.defaults());
    }

    /**
     * Routing-aware constructor. {@code routing} controls how tasks are
     * mapped to shards at submit time (see {@link ShardedRoutingConfig}).
     */
    public ShardedOnlyDispatcher(int workerCount,
                                 WorkerStats[] workerStats,
                                 ShardedRoutingConfig routing) {
        this(workerCount, workerStats, routing, null);
    }

    /** Full constructor with optional per-worker busy/idle tracker. */
    public ShardedOnlyDispatcher(int workerCount,
                                 WorkerStats[] workerStats,
                                 ShardedRoutingConfig routing,
                                 WorkerBusyIdleTracker busyIdle) {
        this.executor = new ShardedExecutor(workerCount, workerStats,
                routing == null ? ShardedRoutingConfig.defaults() : routing,
                busyIdle);
    }

    @Override
    public void submit(Task task) throws InterruptedException {
        task.markEnqueued();
        executor.submit(task);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public String label() {
        return "ShardedOnly";
    }

    @Override
    public int totalQueueSize() {
        return executor.getTotalQueueSize();
    }

    /** Exposes the backing executor for diagnostics (queue distribution, counts). */
    public ShardedExecutor executor() {
        return executor;
    }
}

