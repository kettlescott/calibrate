package com.scott;

/**
 * Strategy for deriving each task's workload seed from its taskId.
 *
 * <ul>
 *   <li>{@link #SEQUENTIAL_TASK_ID} — legacy:
 *       {@code taskSeed = seed + taskId}. Adjacent taskIds produce
 *       adjacent seeds; combined with {@code MODULO} routing this
 *       binds each shard to a fixed residue class of seeds.</li>
 *   <li>{@link #MIXED_TASK_ID} —
 *       {@code taskSeed = seed ^ mix64(taskId ^ workloadSeed)}.
 *       Decouples adjacent taskIds and taskId residue classes from
 *       any predictable seed pattern.</li>
 * </ul>
 */
public enum WorkloadSeedMode {
    SEQUENTIAL_TASK_ID,
    MIXED_TASK_ID;

    public static WorkloadSeedMode parse(String raw) {
        if (raw == null) return SEQUENTIAL_TASK_ID;
        String r = raw.trim().toUpperCase();
        return switch (r) {
            case "SEQUENTIAL_TASK_ID" -> SEQUENTIAL_TASK_ID;
            case "MIXED_TASK_ID"      -> MIXED_TASK_ID;
            default -> throw new IllegalArgumentException(
                    "Unknown workloadSeedMode: " + raw
                            + " (expected SEQUENTIAL_TASK_ID|MIXED_TASK_ID)");
        };
    }
}

