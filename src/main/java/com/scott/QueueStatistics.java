package com.scott;

import java.util.concurrent.BlockingQueue;

/**
 * Per-queue statistics for shared queue diagnostics.
 *
 * <p>Tracks:
 * <ul>
 *   <li>Enqueued / dequeued task count</li>
 *   <li>Queue depth samples (instantaneous measurements)</li>
 *   <li>Non-empty ratio (fraction of samples when queue had tasks)</li>
 * </ul>
 *
 * <p>Updated by the dispatcher on enqueue, and sampled by diagnostics collectors.
 * Thread-safe via volatile fields for the enqueued/dequeued counters.
 */
final class QueueStatistics {

    private final String queueName;
    private final BlockingQueue<?> queue;

    // Counters updated by dispatcher (via AtomicLong or volatile)
    volatile long enqueuedCount;
    volatile long dequeuedCount;

    // Sampled depth metrics
    long depthSampleCount;
    long depthSummed;
    long depthMaxSampled = Long.MIN_VALUE;
    long nonEmptySampleCount;

    QueueStatistics(String queueName, BlockingQueue<?> queue) {
        this.queueName = queueName;
        this.queue = queue;
    }

    /**
     * Call periodically (e.g., per window) to sample queue depth.
     * Single-writer: called only from diagnostics sampler thread.
     */
    void sampleDepth() {
        if (queue == null) return;
        int size = queue.size();
        depthSampleCount++;
        depthSummed += size;
        if (size > depthMaxSampled) depthMaxSampled = size;
        if (size > 0) {
            nonEmptySampleCount++;
        }
    }

    // ---- Accessors ----

    String queueName() { return queueName; }
    long enqueuedCount() { return enqueuedCount; }
    long dequeuedCount() { return dequeuedCount; }
    long avgDepth() { return depthSampleCount > 0 ? depthSummed / depthSampleCount : 0; }
    long maxDepth() { return depthMaxSampled == Long.MIN_VALUE ? 0 : depthMaxSampled; }
    double nonEmptyRatio() {
        return depthSampleCount > 0 ? (double) nonEmptySampleCount / depthSampleCount : 0.0;
    }

    void incrementEnqueued() {
        enqueuedCount++;
    }

    void incrementDequeued() {
        dequeuedCount++;
    }

    @Override
    public String toString() {
        return String.format(
            "%s: enqueued=%d, dequeued=%d, avgDepth=%d, maxDepth=%d, nonEmptyRatio=%.2f",
            queueName, enqueuedCount, dequeuedCount, avgDepth(), maxDepth(), nonEmptyRatio());
    }
}

