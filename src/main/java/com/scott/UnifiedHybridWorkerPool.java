package com.scott;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unified hybrid worker pool: single set of workerCount threads handling
 * both shared and sharded queues.
 *
 * <p>Architecture:
 * <ul>
 *   <li>1 shared queue (LinkedBlockingQueue)</li>
 *   <li>workerCount sharded queues</li>
 *   <li>workerCount unified worker threads (fixed, never created/destroyed)</li>
 *   <li>Each worker has a ThreadLocal WORKER_ID (0..workerCount-1)</li>
 *   <li>Each worker has a corresponding sample buffer (1:1 mapping)</li>
 * </ul>
 *
 * <p>Workers consume from queues based on current ExecutorState:
 * <ul>
 *   <li>SHARED: poll from shared queue</li>
 *   <li>SHARDED: poll from shard[workerID]</li>
 *   <li>TO_SHARED/TO_SHARDED: drain both during transition</li>
 * </ul>
 *
 * <p>Bidirectional draining ensures no samples are lost during transitions.
 */
public final class UnifiedHybridWorkerPool {

    public static final ThreadLocal<Integer> WORKER_ID = new ThreadLocal<>();

    private final int workerCount;
    private final LinkedBlockingQueue<Task> sharedQueue;
    private final LinkedBlockingQueue<Task>[] shardedQueues;
    private final UnifiedWorker[] workers;
    private final Thread[] workerThreads;
    private final AtomicBoolean shutdown;
    private final AtomicReference<ExecutorState> currentState;

    @SuppressWarnings("unchecked")
    public UnifiedHybridWorkerPool(int workerCount) {
        this(workerCount, ExecutorState.SHARDED);
    }

    @SuppressWarnings("unchecked")
    public UnifiedHybridWorkerPool(int workerCount, ExecutorState initialState) {
        this.workerCount = workerCount;
        this.sharedQueue = new LinkedBlockingQueue<>();
        this.shardedQueues = new LinkedBlockingQueue[workerCount];
        for (int i = 0; i < workerCount; i++) {
            shardedQueues[i] = new LinkedBlockingQueue<>();
        }
        this.workers = new UnifiedWorker[workerCount];
        this.workerThreads = new Thread[workerCount];
        this.shutdown = new AtomicBoolean(false);
        this.currentState = new AtomicReference<>(initialState == null ? ExecutorState.SHARDED : initialState);

        // Create and start unified worker threads
        for (int i = 0; i < workerCount; i++) {
            workers[i] = new UnifiedWorker(i, sharedQueue, shardedQueues, currentState);
            workerThreads[i] = new Thread(workers[i], "HybridWorker-" + i);
            workerThreads[i].start();
        }
    }

    /**
     * Submits a task to the shared queue.
     */
    public void submitShared(Task task) throws InterruptedException {
        if (shutdown.get()) {
            throw new IllegalStateException("Worker pool is shutdown");
        }
        sharedQueue.put(task);
    }

    /**
     * Submits a task to the appropriate shard based on task ID.
     */
    public void submitSharded(Task task) throws InterruptedException {
        if (shutdown.get()) {
            throw new IllegalStateException("Worker pool is shutdown");
        }
        int shardIdx = (int) (task.routingKey() % workerCount);
        shardedQueues[shardIdx].put(task);
    }

    /**
     * Performs bidirectional draining during transition.
     * Coordinates between source and target queues to ensure
     * all pending tasks complete before switching consumption logic.
     */
    public void performTransition(ExecutorState from, ExecutorState to) {
        // Signal all workers to start drain mode
        for (UnifiedWorker worker : workers) {
            worker.setTransitionPending(true);
        }

        // Wait for all workers to acknowledge drain mode
        long drainStartNanos = System.nanoTime();
        long drainTimeoutNanos = 5_000_000_000L; // 5 seconds
        while (true) {
            boolean allDrained = true;
            for (UnifiedWorker worker : workers) {
                if (!worker.isDrained()) {
                    allDrained = false;
                    break;
                }
            }
            if (allDrained) {
                break;
            }
            if (System.nanoTime() - drainStartNanos > drainTimeoutNanos) {
                System.err.println("[Hybrid] WARNING: transition drain timeout after 5s");
                break;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Update current state for workers to consume from new mode
        currentState.set(to);

        // Signal transition complete
        for (UnifiedWorker worker : workers) {
            worker.setTransitionPending(false);
        }
    }

    /**
     * Shuts down the worker pool.
     */
    public void shutdown() {
        shutdown.set(true);
        
        // Drain remaining tasks
        Task task;
        while ((task = sharedQueue.poll()) != null) {
            // Discard
        }
        for (LinkedBlockingQueue<Task> queue : shardedQueues) {
            while ((task = queue.poll()) != null) {
                // Discard
            }
        }

        // Interrupt all worker threads
        for (Thread thread : workerThreads) {
            thread.interrupt();
        }
    }

    /**
     * Waits for worker pool termination.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        for (Thread thread : workerThreads) {
            long remaining = Math.max(0, deadlineNanos - System.nanoTime());
            long remainingMillis = unit.convert(remaining, TimeUnit.NANOSECONDS);
            thread.join(remainingMillis);
            if (thread.isAlive()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true when the shared queue has no pending tasks.
     */
    public boolean isSharedQueueEmpty() {
        return sharedQueue.isEmpty();
    }

    /**
     * Returns true when all shard queues are empty.
     */
    public boolean areAllShardsEmpty() {
        for (LinkedBlockingQueue<Task> queue : shardedQueues) {
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Individual worker handling both shared and sharded consumption.
     */
    private static class UnifiedWorker implements Runnable {
        private final int workerID;
        private final LinkedBlockingQueue<Task> sharedQueue;
        private final LinkedBlockingQueue<Task>[] shardedQueues;
        private final AtomicReference<ExecutorState> currentState;
        private volatile boolean transitionPending = false;
        private volatile boolean drained = false;

        UnifiedWorker(int workerID, LinkedBlockingQueue<Task> sharedQueue,
                     LinkedBlockingQueue<Task>[] shardedQueues,
                     AtomicReference<ExecutorState> currentState) {
            this.workerID = workerID;
            this.sharedQueue = sharedQueue;
            this.shardedQueues = shardedQueues;
            this.currentState = currentState;
        }

        void setTransitionPending(boolean pending) {
            if (pending) {
                // Always start a transition with a clean drain flag. A stale
                // "true" left over from a previous transition would make the
                // next performTransition believe this worker is already
                // drained and skip it — stranding tasks in the old queue.
                this.drained = false;
            }
            this.transitionPending = pending;
            if (!pending) {
                this.drained = false;  // Reset drained flag when transition ends
            }
        }

        boolean isDrained() {
            return drained;
        }

        @Override
        public void run() {
            WORKER_ID.set(workerID);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (transitionPending) {
                        // Drain both queues during transition
                        drainBothQueues();
                        drained = true;
                    } else {
                        // Normal consumption based on current state
                        ExecutorState state = currentState.get();
                        consumeByState(state);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private void drainBothQueues() throws InterruptedException {
            Task task;
            // Keep draining both queues until empty
            while (true) {
                task = sharedQueue.poll();
                if (task != null) {
                    task.run();
                    continue;
                }
                task = shardedQueues[workerID].poll();
                if (task != null) {
                    task.run();
                    continue;
                }
                // Both queues empty
                break;
            }
        }

        private void consumeByState(ExecutorState state) throws InterruptedException {
            if (state == ExecutorState.SHARED || state == ExecutorState.TO_SHARED) {
                // Primary: shared queue.
                Task task = sharedQueue.poll(10, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                } else {
                    // Safety net: shared queue was momentarily empty. Drain any
                    // task that may have been left in this worker's shard by an
                    // imperfect SHARDED->SHARED handoff. In steady state the
                    // shard is empty, so this is a cheap non-blocking poll.
                    Task leftover = shardedQueues[workerID].poll();
                    if (leftover != null) {
                        leftover.run();
                    }
                }
            } else if (state == ExecutorState.SHARDED || state == ExecutorState.TO_SHARDED) {
                // Primary: this worker's shard.
                Task task = shardedQueues[workerID].poll(10, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                } else {
                    // Safety net: drain any task left in the shared queue by an
                    // imperfect SHARED->SHARDED handoff. Steady state: empty.
                    Task leftover = sharedQueue.poll();
                    if (leftover != null) {
                        leftover.run();
                    }
                }
            }
        }
    }
}
