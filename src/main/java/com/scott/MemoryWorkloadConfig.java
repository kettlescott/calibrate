package com.scott;

/**
 * Optional MEMORY-kind configuration carried by a {@link WorkloadEntry}.
 *
 * <p>Lets YAML control the experimental knobs that materially change
 * memory-hierarchy behaviour without touching code:
 * <ul>
 *   <li>{@code accessPattern} — {@code SEQUENTIAL} (cache-friendly) vs.
 *       {@code RANDOM} (TLB / LLC pressure).</li>
 *   <li>{@code bufferMB} — working-set size in MiB. Typical ladder for
 *       memory experiments: 8 / 64 / 256 (fits in L2 / overflows L3 /
 *       fully off-cache on most server CPUs).</li>
 *   <li>{@code writeBack} — when {@code true} the workload writes back
 *       to the shared buffer (introduces false sharing + write-back
 *       traffic). Defaults to {@code false} so the default benchmark
 *       isolates pure read pressure from queue / dispatcher behaviour.</li>
 * </ul>
 *
 * <p>This is read by {@link TaskGenerator} when an entry's
 * {@link WorkloadKind} is {@link WorkloadKind#MEMORY}; ignored
 * otherwise.
 *
 * <p>YAML example:
 * <pre>{@code
 * - kind: MEMORY
 *   targetMillis: 5
 *   ratio: 1.0
 *   memory:
 *     accessPattern: RANDOM
 *     bufferMB: 64
 *     writeBack: false
 * }</pre>
 */
public record MemoryWorkloadConfig(
        MemoryBoundWorkload.AccessPattern accessPattern,
        int bufferMB,
        boolean writeBack
) {

    public static final MemoryBoundWorkload.AccessPattern DEFAULT_PATTERN =
            MemoryBoundWorkload.AccessPattern.SEQUENTIAL;

    /** Default working-set size in MiB. ≈ 8 MiB (1 &lt;&lt; 20 longs). */
    public static final int DEFAULT_BUFFER_MB = 8;

    public static final boolean DEFAULT_WRITE_BACK = false;

    public static MemoryWorkloadConfig defaults() {
        return new MemoryWorkloadConfig(DEFAULT_PATTERN, DEFAULT_BUFFER_MB, DEFAULT_WRITE_BACK);
    }

    @SuppressWarnings("unchecked")
    public static MemoryWorkloadConfig fromMap(java.util.Map<String, Object> map) {
        if (map == null) {
            return defaults();
        }
        String patternStr = map.get("accessPattern") == null ? null
                : String.valueOf(map.get("accessPattern")).toUpperCase();
        MemoryBoundWorkload.AccessPattern pattern = patternStr == null
                ? DEFAULT_PATTERN
                : MemoryBoundWorkload.AccessPattern.valueOf(patternStr);
        
        Object bufMb = map.get("bufferMB");
        int bufferMB = bufMb == null ? DEFAULT_BUFFER_MB
                : Integer.parseInt(String.valueOf(bufMb));
        
        Object wb = map.get("writeBack");
        boolean writeBack = wb == null ? DEFAULT_WRITE_BACK
                : Boolean.parseBoolean(String.valueOf(wb));
        
        return new MemoryWorkloadConfig(pattern, bufferMB, writeBack);
    }

    public MemoryWorkloadConfig {
        if (accessPattern == null) {
            accessPattern = DEFAULT_PATTERN;
        }
        if (bufferMB <= 0) {
            throw new IllegalArgumentException(
                    "memory.bufferMB must be > 0 (got " + bufferMB + ")");
        }
    }

    /** Buffer size expressed in {@code long}s (8 bytes each). */
    public int bufferLongs() {
        // bufferMB MiB / 8 B per long = bufferMB * 1<<17 longs
        long longs = (long) bufferMB * (1L << 17);
        if (longs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "memory.bufferMB too large (would overflow int): " + bufferMB);
        }
        return (int) longs;
    }

    public String summary() {
        return "accessPattern=" + accessPattern
                + ", bufferMB=" + bufferMB
                + ", writeBack=" + writeBack;
    }
}

