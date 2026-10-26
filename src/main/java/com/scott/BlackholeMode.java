package com.scott;

/**
 * Sink strategy used by {@link MemoryBoundWorkload#execute()} to
 * defeat dead-store elimination of the loop result.
 *
 * <ul>
 *   <li>{@link #SHARED_VOLATILE} — legacy: a single
 *       {@code static volatile long} field. All worker threads store
 *       into the same cache line, causing cross-core invalidation
 *       traffic that pollutes memory-bound measurements.</li>
 *   <li>{@link #THREAD_LOCAL} — per-thread {@code long[]} sink. No
 *       cross-thread cache-line sharing; still opaque enough to the
 *       JIT (escaping through {@code ThreadLocal.get()}) to prevent
 *       dead-store elimination of the loop result.</li>
 * </ul>
 */
public enum BlackholeMode {
    SHARED_VOLATILE,
    THREAD_LOCAL;

    public static BlackholeMode parse(String raw) {
        if (raw == null) return SHARED_VOLATILE;
        String r = raw.trim().toUpperCase();
        return switch (r) {
            case "SHARED_VOLATILE" -> SHARED_VOLATILE;
            case "THREAD_LOCAL"    -> THREAD_LOCAL;
            default -> throw new IllegalArgumentException(
                    "Unknown blackholeMode: " + raw
                            + " (expected SHARED_VOLATILE|THREAD_LOCAL)");
        };
    }
}

