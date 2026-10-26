package com.scott;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * HYBRID executor: fixed queues with adaptive SHARED/SHARDED switching based on CV.
 *
 * <p>True unified architecture:
 * <ul>
 *   <li>1 shared queue (LinkedBlockingQueue)</li>
 *   <li>workerCount sharded queues</li>
 *   <li>workerCount unified worker threads (fixed, never created/destroyed)</li>
 *   <li>workerCount sample buffers (1:1 mapping with worker threads)</li>
 *   <li>1 independent CV monitor thread</li>
 * </ul>
 *
 * <p>Each worker thread is permanently assigned a worker ID (0..workerCount-1) via ThreadLocal.
 * This ID directly maps to a sample buffer. Switching only changes routing/consumption logic,
 * not thread creation.
 *
 * <p>Task routing:
 * <ul>
 *   <li>SHARDED: route to shard[taskId % workerCount]</li>
 *   <li>SHARED: route to single shared queue</li>
 *   <li>TO_SHARED/TO_SHARDED: transitional states (workers handle both)</li>
 * </ul>
 *
 * Switching is coordinated via atomic state and volatile flags.
 */
public final class AdaptiveHybridExecutor implements BenchmarkExecutor {

    private final int workerCount;
    private final AdaptiveHybridConfig config;
    private final UnifiedHybridWorkerPool workerPool;
    private final SampleBuffer[] sampleBuffers;
    private final CVMonitorThread monitorThread;
    private final AtomicReference<ExecutorState> currentState;

    private static final long STATS_DWELL_NANOS = TimeUnit.SECONDS.toNanos(3);

    private volatile boolean shutdown = false;
    private long measurementStartNanos;
    private volatile long statsStableAfterNanos;

    public AdaptiveHybridExecutor(int workerCount, AdaptiveHybridConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("AdaptiveHybridConfig must not be null");
        }

        this.workerCount = workerCount;
        this.config = config;

        // Prepare sample buffers for each worker before creating worker pool
        this.sampleBuffers = new SampleBuffer[workerCount];
        for (int i = 0; i < workerCount; i++) {
            sampleBuffers[i] = new SampleBuffer();
        }

        // Wire global completion sink to route samples to buffers by worker ID
        Task.setCompletionSampleSink((task, threadName) -> {
            int workerID = UnifiedHybridWorkerPool.WORKER_ID.get();
            if (workerID >= 0 && workerID < sampleBuffers.length) {
                sampleBuffers[workerID].offerSample(task.finishNanos(), task.executionTimeNanos());
            }
        });

        // Initialize state
        ExecutorState initialState = config.initialMode() == ExecutorMode.SHARED 
                ? ExecutorState.SHARED : ExecutorState.SHARDED;
        this.currentState = new AtomicReference<>(initialState);

        // Create unified worker pool with the same initial consumer state
        this.workerPool = new UnifiedHybridWorkerPool(workerCount, initialState);

        // Create monitor thread
        this.measurementStartNanos = System.nanoTime();
        this.statsStableAfterNanos = this.measurementStartNanos;
        this.monitorThread = new CVMonitorThread(
                sampleBuffers,
                config,
                measurementStartNanos,
                currentState,
                this::onSwitchRequested
        );
    }


    /**
     * Marks the start of measurement phase (after warmup).
     * Called by benchmark runner to enable CV monitoring.
     */
    public void markMeasurementStart() {
        this.measurementStartNanos = System.nanoTime();
        this.statsStableAfterNanos = this.measurementStartNanos;
        System.out.printf("[Hybrid] start monitoring: initialMode=%s, metric=CV, "
                + "sharedEnterCv=%.3f, shardedEnterCv=%.3f, windowMillis=%d, debugCvPrint=%ds%n",
                currentState.get().displayName(),
                config.sharedEnterCv(), config.shardedEnterCv(), 
                config.windowMillis(), config.debugCvPrintIntervalSeconds());
        monitorThread.start();
    }

    @Override
    public void submit(Task task) throws InterruptedException {
        if (shutdown) {
            throw new IllegalStateException("Executor is shutdown");
        }

        ExecutorState state = currentState.get();

        switch (state) {
            case SHARDED:
                workerPool.submitSharded(task);
                break;
            case SHARED:
                workerPool.submitShared(task);
                break;
            case TO_SHARED:
                // During transition to SHARED: route new tasks to SHARED
                // This prevents accumulation in both queues during drain
                workerPool.submitShared(task);
                break;
            case TO_SHARDED:
                // During transition to SHARDED: route new tasks to SHARDED
                // This prevents accumulation in both queues during drain
                workerPool.submitSharded(task);
                break;
        }
    }

    /**
     * Called by monitor thread when a switch is requested.
     * Uses bidirectional draining to coordinate handoff between modes.
     */
    private void onSwitchRequested(ExecutorState from, ExecutorState to,
                                   double metric, long mean, long sampleCount) {
        // Temporarily suppress metric recording during active transition.
        this.statsStableAfterNanos = Long.MAX_VALUE;
        long transitionStartNanos = System.nanoTime();
        double transitionStartSecs = (transitionStartNanos - measurementStartNanos) / 1_000_000_000.0;
        String fromMode = from == ExecutorState.SHARED ? "shared" : "sharded";
        String toMode = to == ExecutorState.SHARED ? "shared" : "sharded";
        String metricLabel = "cv";

        long[] minMax = computeAggregateMinMaxForSwitchLog();
        double minMicros = minMax[0] / 1000.0;
        double maxMicros = minMax[1] / 1000.0;

        System.out.printf("[Hybrid] t=%.3fs start switching from %s to %s, %s=%.4f, "
                + "threshold=%.4f, mean=%.1fus, min=%.1fus, max=%.1fus, samples=%d%n",
                transitionStartSecs,
                fromMode,
                toMode,
                metricLabel,
                metric,
                (from == ExecutorState.SHARDED) ? config.sharedEnterCv() : config.shardedEnterCv(),
                mean / 1000.0,
                minMicros,
                maxMicros,
                sampleCount
        );

        // Set transitional state
        ExecutorState transState = (to == ExecutorState.SHARED) ? ExecutorState.TO_SHARED : ExecutorState.TO_SHARDED;
        currentState.set(transState);

        // Bidirectional draining: coordinate source and target queue handoff
        workerPool.performTransition(from, to);

        // Complete transition
        currentState.set(to);

        long transitionEndNanos = System.nanoTime();
        // Wait briefly after handoff so summaries capture only stable mode behavior.
        this.statsStableAfterNanos = transitionEndNanos + STATS_DWELL_NANOS;
        double transitionEndSecs = (transitionEndNanos - measurementStartNanos) / 1_000_000_000.0;
        long transitionTimeMs = (transitionEndNanos - transitionStartNanos) / 1_000_000L;

        System.out.printf("[Hybrid] t=%.3fs switch completed: %s -> %s, transition_time=%dms%n",
                transitionEndSecs,
                fromMode,
                toMode,
                transitionTimeMs
        );
    }

    @Override
    public void shutdown() {
        shutdown = true;
        if (Task.completionSampleSink() != null) {
            Task.setCompletionSampleSink(null);
        }
        monitorThread.stopMonitoring();
        workerPool.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);

        // Wait for monitor thread
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining > 0) {
            monitorThread.join(unit.toMillis(timeout));
        }

        // Wait for worker pool
        remaining = deadlineNanos - System.nanoTime();
        return workerPool.awaitTermination(Math.max(remaining, 0), TimeUnit.NANOSECONDS);
    }

    /**
     * Returns current executor state.
     */
    public ExecutorState getCurrentState() {
        return currentState.get();
    }

    /**
     * Returns sample buffers for external access (testing/diagnostics).
     */
    public SampleBuffer[] getSampleBuffers() {
        return sampleBuffers;
    }

    /**
     * Returns monitor thread for diagnostics.
     */
    public CVMonitorThread getMonitorThread() {
        return monitorThread;
    }

    /**
     * Returns true when a completed measurement task is considered stable enough
     * for phase summaries (outside transition and after short post-switch dwell).
     */
    public boolean shouldRecordStableMeasurement(Task task) {
        if (task == null || !task.isMeasurement()) {
            return false;
        }

        ExecutorState state = currentState.get();
        if (!state.isStable()) {
            return false;
        }
        if (task.finishNanos() < statsStableAfterNanos) {
            return false;
        }

        if (state == ExecutorState.SHARED) {
            return workerPool.areAllShardsEmpty();
        }
        return workerPool.isSharedQueueEmpty();
    }

    /**
     * Returns total queue depth (for compatibility).
     * Returns -1 as unsupported for adaptive executor.
     */
    public int totalQueueSize() {
        return -1;  // Unsupported
    }

    /**
     * Returns the latest aggregate min/max window values from the monitor thread.
     * If no samples are available yet, returns [0, 0].
     */
    private long[] computeAggregateMinMaxForSwitchLog() {
        long min = monitorThread.getLastMinNanos();
        long max = monitorThread.getLastMaxNanos();
        if (min == 0L && max == 0L) {
            return new long[] { 0L, 0L };
        }
        return new long[] { min, max };
    }
}
