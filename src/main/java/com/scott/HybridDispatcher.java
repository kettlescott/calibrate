package com.scott;

import java.util.concurrent.TimeUnit;

/**
 * Dispatcher that routes by {@link WorkloadKind} according to a
 * fully YAML-driven {@link HybridConfig}.
 *
 * <p>There are <strong>no built-in defaults</strong>. The routing
 * policy (which kinds go to the shared executor vs. the sharded
 * executor) is read entirely from configuration. This makes routing
 * policy a first-class experimental variable for paper experiments
 * such as:
 * <ul>
 *   <li>Policy A: CPU-&gt;SHARDED, MEMORY-&gt;SHARED</li>
 *   <li>Policy B: CPU-&gt;SHARED,  MEMORY-&gt;SHARDED</li>
 *   <li>Policy C: CPU-&gt;SHARDED, MEMORY-&gt;SHARDED</li>
 *   <li>Policy D: CPU-&gt;SHARED,  MEMORY-&gt;SHARED</li>
 * </ul>
 *
 * <p>Routing reduces to a single {@link java.util.EnumMap} lookup
 * (constant-time, no synchronization, no allocation) plus a switch on
 * the resulting target.
 */
public final class HybridDispatcher implements Dispatcher {

    private final HybridConfig config;
    private final SharedExecutor sharedExecutor;
    private final ShardedExecutor shardedExecutor;

    public HybridDispatcher(HybridConfig config) {
        if (config == null) {
            throw new IllegalArgumentException(
                    "HybridDispatcher requires a HybridConfig - there are no built-in defaults. "
                            + "Provide a 'hybrid:' section in YAML with sharedWorkers, shardedWorkers, "
                            + "and an explicit routing for every WorkloadKind.");
        }
        config.validate();
        this.config          = config;
        this.sharedExecutor  = new SharedExecutor(config.sharedWorkers());
        this.shardedExecutor = new ShardedExecutor(config.shardedWorkers());
    }

    @Override
    public void submit(Task task) throws InterruptedException {
        // Resolve the routing target BEFORE stamping the enqueued
        // timestamp, so YAML-driven routing lookup is attributed to
        // submit overhead — not to queue wait. Otherwise Hybrid runs
        // would systematically inflate queueWaitTimeNanos relative to
        // SharedOnly / ShardedOnly, biasing cross-mode comparisons.
        HybridConfig.RouteTarget target = config.routeFor(task.workloadKind());
        task.markEnqueued();
        switch (target) {
            case SHARED  -> sharedExecutor.submit(task);
            case SHARDED -> shardedExecutor.submit(task);
        }
    }

    @Override
    public void shutdown() {
        sharedExecutor.shutdown();
        shardedExecutor.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);

        long remaining = deadlineNanos - System.nanoTime();
        boolean sharedDone = sharedExecutor.awaitTermination(
                Math.max(remaining, 0), TimeUnit.NANOSECONDS);

        remaining = deadlineNanos - System.nanoTime();
        boolean shardedDone = shardedExecutor.awaitTermination(
                Math.max(remaining, 0), TimeUnit.NANOSECONDS);

        return sharedDone && shardedDone;
    }

    @Override
    public String label() { return "Hybrid"; }

    @Override
    public int totalQueueSize() {
        return sharedExecutor.getQueueSize() + shardedExecutor.getTotalQueueSize();
    }
}

