package com.scott;

import java.util.Arrays;

/**
 * A primitive {@code long} dynamic array designed for benchmark use.
 *
 * <p>Stores raw {@code long} values in a backing {@code long[]} array,
 * completely avoiding {@link Long} boxing and the associated object
 * allocation overhead that would pollute young-generation GC during
 * latency measurement.
 *
 * <h3>Capacity policy</h3>
 * <ul>
 *   <li>Callers that know the sample count up front should pass it as
 *       {@code initialCapacity} so the buffer never needs to grow.</li>
 *   <li>If the buffer does fill up, {@link #add(long)} automatically
 *       grows the backing array by 1.5×.  This is a safety net —
 *       production benchmarks should size the buffer correctly to
 *       avoid allocation on the hot path.</li>
 * </ul>
 */
public final class LongBuffer {

    private static final int DEFAULT_CAPACITY = 16_384;

    private long[] data;
    private int size;

    /**
     * Creates a buffer with the default initial capacity (1024).
     */
    public LongBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a buffer pre-sized to hold {@code initialCapacity} samples.
     *
     * @param initialCapacity expected number of samples; must be &gt; 0
     */
    public LongBuffer(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive, got " + initialCapacity);
        }
        this.data = new long[initialCapacity];
        this.size = 0;
    }

    /* ---- mutators ---- */

    /**
     * Appends a primitive {@code long} value — no boxing occurs.
     */
    public void add(long value) {
        if (size == data.length) {
            grow();
        }
        data[size++] = value;
    }

    /* ---- queries ---- */

    /** Returns the number of values stored. */
    public int size() {
        return size;
    }

    /** Returns {@code true} if no values have been added. */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the value at the given index.
     *
     * @throws IndexOutOfBoundsException if index is out of range
     */
    public long get(int index) {
        checkIndex(index);
        return data[index];
    }

    /**
     * Returns a <em>copy</em> of the stored data as a new {@code long[]}
     * whose length equals {@link #size()}.
     */
    public long[] toArray() {
        return Arrays.copyOf(data, size);
    }

    /* ---- internals ---- */

    /** Grows the backing array by ~1.5× (minimum +1). */
    private void grow() {
        int newCapacity = data.length + (data.length >> 1);
        if (newCapacity <= data.length) {
            newCapacity = data.length + 1;   // overflow guard
        }
        data = Arrays.copyOf(data, newCapacity);
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(
                    "index " + index + " out of range [0, " + size + ")");
        }
    }
}

