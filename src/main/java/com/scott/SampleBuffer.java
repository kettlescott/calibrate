package com.scott;

/**
 * Per-worker bounded SPSC (single-producer/single-consumer) sample buffer.
 *
 * <p>Each worker thread writes task execution samples (completionNanos, executionNanos)
 * into its own buffer. The monitor thread reads these samples without blocking.
 *
 * <p>Only the owning worker writes; only the monitor thread reads.
 * Uses volatile fields to ensure proper cross-thread publication semantics.
 *
 * <p>Protocol:
 * <ul>
 *   <li>Writer (producer): updates completionNanos[writeIndex] and executionNanos[writeIndex],
 *       then updates volatile writeIndex (release semantics)</li>
 *   <li>Reader (consumer): reads volatile writeIndex (acquire semantics),
 *       then reads sample data, then updates readIndex</li>
 * </ul>
 *
 * <p>When the buffer is full, new samples are dropped and a counter is incremented.
 *
 * <p>Uses primitive arrays to avoid GC pressure.
 */
public final class SampleBuffer {
    private static final int DEFAULT_CAPACITY = 65536;  // Increased from 32768

    private final long[] completionNanos;
    private final long[] executionNanos;
    private final int capacity;
    
    // Volatile to ensure proper cross-thread publication
    // Writer updates writeIndex last (release)
    // Reader reads writeIndex first (acquire), then readIndex (release)
    private volatile int writeIndex = 0;
    private volatile int readIndex = 0;

    private volatile long droppedCount = 0L;
    
    // Track actual dropped samples from buffer (overflow)
    // This is distinct from capacity limit drops and is always accurate
    private long droppedSamples = 0L;

    public SampleBuffer(int capacity) {
        this.capacity = capacity;
        this.completionNanos = new long[capacity];
        this.executionNanos = new long[capacity];
        this.writeIndex = 0;
        this.readIndex = 0;
    }

    public SampleBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Worker thread (producer): writes a sample if the buffer has space.
     * If full, increments droppedCount and returns without writing.
     *
     * <p>Protocol: write sample data, then publish writeIndex (volatile store = release).
     *
     * @param completionNanos the nanoTime when task completed
     * @param executionNanos the actual execution duration in nanos
     */
    public void offerSample(long completionNanos, long executionNanos) {
        int nextWrite = (writeIndex + 1) % capacity;
        if (nextWrite == readIndex) {
            // Buffer full: sample dropped
            droppedCount++;
            droppedSamples++;
            return;
        }
        // Write sample data first (plain store, before volatile write)
        this.completionNanos[writeIndex] = completionNanos;
        this.executionNanos[writeIndex] = executionNanos;
        // Then publish by updating volatile writeIndex (release semantics)
        this.writeIndex = nextWrite;
    }

    /**
     * Monitor thread (consumer): reads one sample without blocking.
     * Returns null if buffer is empty.
     *
     * <p>Protocol: check volatile writeIndex (acquire), read data, advance readIndex.
     *
     * @return a sample, or null if no samples available
     */
    public Sample pollSample() {
        // Read volatile writeIndex (acquire semantics ensures data visibility)
        int write = this.writeIndex;
        if (readIndex == write) {
            return null;  // empty
        }
        // Now safe to read sample data
        long completion = completionNanos[readIndex];
        long execution = executionNanos[readIndex];
        readIndex = (readIndex + 1) % capacity;
        return new Sample(completion, execution);
    }

    /**
     * Checks if buffer has any unread samples.
     */
    public boolean hasData() {
        return readIndex != writeIndex;
    }

    /**
     * Total number of samples dropped due to buffer overflow.
     * Tracks only samples that failed to fit in the current capacity.
     */
    public long droppedSampleCount() {
        return droppedCount;
    }

    /**
     * Returns the actual count of samples dropped due to buffer overflow.
     * This is always accurate and separate from capacity management.
     */
    public long getActualDroppedSamples() {
        return droppedSamples;
    }

    /**
     * Resets the buffer state (for new measurement phase).
     * Must only be called when no producer or consumer is active.
     * 
     * <p>Unsafe to call while either thread is accessing this buffer.
     * Caller must ensure both are stopped.
     */
    public void reset() {
        // This is a write operation that requires mutual exclusion
        // Caller is responsible for ensuring both producer and consumer are stopped
        writeIndex = 0;
        readIndex = 0;
        droppedCount = 0L;
        droppedSamples = 0L;
    }

    /**
     * One sample: completion time and execution duration.
     */
    public record Sample(long completionNanos, long executionNanos) {}
}
