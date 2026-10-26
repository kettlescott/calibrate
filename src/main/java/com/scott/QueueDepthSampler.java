package com.scott;

/**
 * Background queue-depth sampler.
 *
 * <p>Periodically polls {@link Dispatcher#totalQueueSize()} on a single
 * daemon thread and aggregates the running sum / count / max so the
 * benchmark can report {@code avgQueueDepth} and {@code maxQueueDepth}
 * across the measurement window without per-sample allocation or
 * synchronization.
 *
 * <p>Implementation notes:
 * <ul>
 *   <li>Sampler runs on its own daemon thread; the benchmark hot path
 *       is unaffected.</li>
 *   <li>Aggregates are updated by the sampler thread only; the
 *       benchmark reads them after {@link #stop()} joins, so no
 *       happens-before issue beyond the join.</li>
 *   <li>{@code -1} samples (dispatcher reports "unknown") are skipped.</li>
 * </ul>
 */
public final class QueueDepthSampler {

    private final Dispatcher dispatcher;
    private final long intervalNanos;
    private final Thread thread;
    private volatile boolean running = false;

    private long sampleCount;
    private long sampleSum;
    private int  sampleMax = Integer.MIN_VALUE;

    public QueueDepthSampler(Dispatcher dispatcher, long intervalMillis) {
        this.dispatcher = dispatcher;
        this.intervalNanos = Math.max(1L, intervalMillis) * 1_000_000L;
        this.thread = new Thread(this::loop, "QueueDepthSampler");
        this.thread.setDaemon(true);
    }

    public void start() {
        running = true;
        thread.start();
    }

    public void stop() {
        running = false;
        thread.interrupt();
        try {
            thread.join(1_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        long next = System.nanoTime();
        while (running) {
            int depth = dispatcher.totalQueueSize();
            if (depth >= 0) {
                sampleCount++;
                sampleSum += depth;
                if (depth > sampleMax) sampleMax = depth;
            }
            next += intervalNanos;
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                java.util.concurrent.locks.LockSupport.parkNanos(sleep);
            } else {
                // We've fallen behind; reset cadence to now to avoid runaway catch-up.
                next = System.nanoTime();
            }
        }
    }

    /** Average queue depth over all valid samples; {@code 0.0} if none. */
    public double avgQueueDepth() {
        return sampleCount == 0 ? 0.0 : (double) sampleSum / sampleCount;
    }

    /** Maximum observed queue depth; {@code 0} if no valid samples. */
    public int maxQueueDepth() {
        return sampleCount == 0 ? 0 : sampleMax;
    }

    public long sampleCount() { return sampleCount; }
}

