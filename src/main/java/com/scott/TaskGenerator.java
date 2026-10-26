package com.scott;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Central factory for per-task {@link Workload} instances, driven by a
 * {@link WorkloadConfig} list of {@link WorkloadEntry}.
 *
 * <p>Per entry, workload state is prepared once at startup:
 * <ul>
 *   <li>CPU    — duration-driven execution from {@code durationFile}</li>
 *   <li>MEMORY — shared {@code long[]} buffer + duration-driven execution</li>
 * </ul>
 *
 * <p>Determinism:
 * <ul>
 *   <li>Workload selection and per-task workload seeds are derived
 *       deterministically from the configured workload seed and the
 *       task ID (SplitMix-style hash of {@code seed ^ taskId}).</li>
 *   <li>Experiment 2 shard-assignment schedules are generated
 *       independently from the configured shard-imbalance seed
 *       ({@link ShardImbalanceConfig#randomSeed()}) and shuffled
 *       deterministically (Fisher-Yates).</li>
 * </ul>
 * Identical configuration and seeds therefore produce identical
 * workload and shard-assignment streams across runs.
 */
public final class TaskGenerator {

    /**
     * Per-{@link WorkloadEntry} calibration record. Surfaced via
     * {@link #calibrations()} so {@link BenchmarkMain} can include the
     * actual calibrated sizes (not just the YAML target) in summary
     * output for reproducibility.
     *
     * <p>Fields that don't apply to a given {@code kind} are zero / null.
     * <ul>
     *   <li>CPU    — duration-file execution; memory fields zero/null</li>
     *   <li>MEMORY — {@code memoryBufferMB},
     *               {@code memoryAccessPattern}, {@code memoryWriteBack},
     *               {@code memoryMode} populated; {@code cpuIterations} = 0</li>
     * </ul>
     */
    public record Calibration(
            String name,
            WorkloadKind kind,
            long targetMillis,
            int cpuIterations,
            boolean fixedCpuIterations,
            long cpuTargetMicros,
            String cpuMode,
            int memorySteps,
            int memoryBufferMB,
            MemoryBoundWorkload.AccessPattern memoryAccessPattern,
            boolean memoryWriteBack,
            boolean fixedMemorySteps,
            long targetMicros,
            String memoryMode
    ) {
        public String summary() {
            return switch (kind) {
                case CPU    -> {
                    yield String.format("name=%s, kind=CPU, durationFile=true", name);
                }
                case MEMORY -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("name=").append(name)
                      .append(", kind=MEMORY, durationFile=true");
                    sb.append(", bufferMB=").append(memoryBufferMB)
                      .append(", accessPattern=").append(memoryAccessPattern)
                      .append(", writeBack=").append(memoryWriteBack);
                    yield sb.toString();
                }
            };
        }
    }

    private static final class EntryState {
        final WorkloadEntry entry;
        final int cpuIterations;                                // CPU fixed-iteration only
        final long cpuTargetMicros;                             // CPU duration-controlled only
        final String cpuMode;                                   // "FIXED_ITERATION" or "DURATION_CONTROLLED"
        final int memorySteps;                                  // MEMORY only
        final long[] memoryBuffer;                              // MEMORY only (shared)
        final MemoryBoundWorkload.AccessPattern memoryPattern;  // MEMORY only
        final boolean memoryWriteBack;                          // MEMORY only
        final long memoryTargetMicros;                          // MEMORY duration-controlled only
        final String memoryMode;                                // "FIXED_STEP" or "DURATION_CONTROLLED"

        EntryState(WorkloadEntry entry, long baseSeed) {
            this.entry = entry;
            switch (entry.kind()) {
                case CPU -> {
                    this.cpuMode = "DURATION_CONTROLLED";
                    this.cpuIterations = 0;
                    this.cpuTargetMicros = 0L;
                    this.memorySteps     = 0;
                    this.memoryBuffer    = null;
                    this.memoryPattern   = null;
                    this.memoryWriteBack = false;
                    this.memoryTargetMicros = 0L;
                    this.memoryMode = null;
                }
                case MEMORY -> {
                    this.cpuIterations = 0;
                    this.cpuTargetMicros = 0L;
                    this.cpuMode = null;
                    
                    MemoryWorkloadConfig mem = entry.memoryOrDefaults();
                    long[] buf = allocateBuffer(mem.bufferLongs(), baseSeed);
                    this.memoryBuffer    = buf;
                    this.memoryPattern   = mem.accessPattern();
                    this.memoryWriteBack = mem.writeBack();

                    this.memoryMode = "DURATION_CONTROLLED";
                    this.memoryTargetMicros = 0L;
                    this.memorySteps = 0;
                }
                default -> throw new IllegalStateException("Unknown WorkloadKind: " + entry.kind());
            }
        }

        private static long[] allocateBuffer(int n, long seed) {
            long[] buf = new long[n];
            long x = seed == 0L ? 0x9E3779B97F4A7C15L : seed;
            for (int i = 0; i < n; i++) {
                x ^= (x << 13); x ^= (x >>> 7); x ^= (x << 17);
                buf[i] = x;
            }
            return buf;
        }
    }

    private final WorkloadConfig workload;
    private final long seed;
    private final EntryState[] states;
    private final double[] cumulative; // normalized cdf, length == states.length
    private final WorkloadSeedMode seedMode;
    private final long workloadSeed;
    private final Path configDir;  // Directory containing the config file, used to resolve relative durationFile paths
    private boolean useHybridWorkloads = false;  // Flag to enable HybridCPUWorkload for CV monitoring

    // NEW: File-mode task generation
    private final DurationFileTaskSource fileSource;  // null for non-file mode

    // ---- Shard-imbalance experiment (Experiment 2) ----
    // Legacy vs Experiment 2 are distinguished ONLY by presence of the
    // configuration object:
    //   imbalance == null  -> legacy behaviour (routingKey == taskId)
    //   imbalance != null  -> Experiment 2 workload path, INCLUDING the
    //                         balanced baseline alpha=0.
    // This guarantees all Experiment 2 alphas share exactly the same
    // task-generation code path; only the probability distribution
    // (and therefore the shuffled shard sequence) changes.
    private final ShardImbalanceConfig imbalance;
    private final int shardCount;
    /**
     * Single deterministic representative routing key per shard, found
     * once at construction by scanning integers through the real
     * {@link Hashing#shardOf}. Guarantees
     * {@code Hashing.shardOf(representativeKeyByShard[s], N, routing) == s}.
     * Removes the need for any RNG or hashing on the per-task path.
     */
    private final long[] representativeKeyByShard;

    // ---- Deterministic shard-target scheduler state (Experiment 2) ----
    // Planned only: planImbalanceSchedule(M) allocates exact per-shard
    // counts via largest-remainder and Fisher-Yates-shuffles them with
    // imbalance.randomSeed(). pickImbalancedRoutingKey() consumes one
    // entry per task. Generating more (or fewer) tasks than planned is
    // a programming error and fails fast.
    private int[] plannedShardSeq;   // null until planImbalanceSchedule() called
    private int   plannedIndex;

    public TaskGenerator(WorkloadConfig workload, long seed) {
        this(workload, seed, WorkloadSeedMode.SEQUENTIAL_TASK_ID, 0L, null);
    }

    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed) {
        this(workload, seed, seedMode, workloadSeed, null, 0, null, null, null);
    }

    /**
     * Extended constructor with config directory support for resolving relative durationFile paths.
     *
     * @param configDir    directory containing the config file; used to resolve relative durationFile paths.
     *                     If null, durationFile paths are treated as absolute.
     */
    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed,
                         Path configDir) {
        this(workload, seed, seedMode, workloadSeed, null, 0, null, configDir, null);
    }

    /**
     * Constructor variant for generated duration sources (no duration file read).
     */
    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed,
                         Path configDir,
                         DurationFileTaskSource injectedFileSource) {
        this(workload, seed, seedMode, workloadSeed, null, 0, null, configDir, injectedFileSource);
    }

    /**
     * Extended constructor supporting the shard-imbalance experiment.
     *
     * <p>Pass {@code imbalance == null} to retain legacy behaviour
     * ({@link Task#routingKey()} stays equal to {@code taskId}).
     * Any non-null imbalance configuration enables the Experiment 2
     * workload path, including {@code alpha == 0}, which represents
     * the balanced experimental baseline. All Experiment 2 alphas —
     * including 0 — go through the exact same task-generation path;
     * only the probability distribution changes.
     *
     * @param imbalance    experiment configuration; {@code null} means legacy mode
     * @param shardCount   number of sharded workers (N); must be &gt; 0 when {@code imbalance != null}
     * @param shardRouting production routing configuration used to precompute per-shard representative keys
     */
    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed,
                         ShardImbalanceConfig imbalance,
                         int shardCount,
                         ShardedRoutingConfig shardRouting) {
        this(workload, seed, seedMode, workloadSeed, imbalance, shardCount, shardRouting, null, null);
    }

    /**
     * Extended constructor supporting the shard-imbalance experiment with config directory.
     *
     * <p>Pass {@code imbalance == null} to retain legacy behaviour
     * ({@link Task#routingKey()} stays equal to {@code taskId}).
     * Any non-null imbalance configuration enables the Experiment 2
     * workload path, including {@code alpha == 0}, which represents
     * the balanced experimental baseline. All Experiment 2 alphas —
     * including 0 — go through the exact same task-generation path;
     * only the probability distribution changes.
     *
     * @param imbalance    experiment configuration; {@code null} means legacy mode
     * @param shardCount   number of sharded workers (N); must be &gt; 0 when {@code imbalance != null}
     * @param shardRouting production routing configuration used to precompute per-shard representative keys
     * @param configDir    directory containing the config file; used to resolve relative durationFile paths.
     *                     If null, durationFile paths are treated as absolute.
     */
    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed,
                         ShardImbalanceConfig imbalance,
                         int shardCount,
                         ShardedRoutingConfig shardRouting,
                         Path configDir) {
        this(workload, seed, seedMode, workloadSeed, imbalance, shardCount, shardRouting, configDir, null);
    }

    public TaskGenerator(WorkloadConfig workload, long seed,
                         WorkloadSeedMode seedMode, long workloadSeed,
                         ShardImbalanceConfig imbalance,
                         int shardCount,
                         ShardedRoutingConfig shardRouting,
                         Path configDir,
                         DurationFileTaskSource injectedFileSource) {
        this.workload = workload;
        this.seed = seed;
        this.seedMode = seedMode == null ? WorkloadSeedMode.SEQUENTIAL_TASK_ID : seedMode;
        this.workloadSeed = workloadSeed;
        this.configDir = configDir;

        // alpha == 0 MUST be a complete no-op that degenerates to the
        // legacy baseline (routingKey == taskId, no planned schedule,
        // no representative-key table). Sharing the imbalance code
        // path at alpha=0 replaces the deterministic round-robin
        // arrival pattern with a Fisher-Yates-randomised one, which
        // shifts the per-shard queueing regime from D/D/1 to M/D/1
        // and invalidates the Experiment 2 control group even though
        // aggregate per-shard task counts remain balanced.
        // See TaskGeneratorImbalanceAlphaZeroTest.
        boolean imbalanceOn = imbalance != null && imbalance.alpha() > 0.0;
        this.imbalance = imbalanceOn ? imbalance : null;
        this.shardCount = imbalanceOn ? shardCount : 0;
        ShardedRoutingConfig effectiveRouting = imbalanceOn
                ? (shardRouting == null ? ShardedRoutingConfig.defaults() : shardRouting)
                : null;
        if (imbalanceOn) {
            if (shardCount <= 0) {
                throw new IllegalArgumentException("shardCount must be > 0 when imbalance is enabled");
            }
            imbalance.validate(shardCount);
            this.representativeKeyByShard = buildRepresentativeKeys(shardCount, effectiveRouting);
        } else {
            this.representativeKeyByShard = null;
        }

        List<WorkloadEntry> entries = workload.entries();
        this.states = new EntryState[entries.size()];
        double sum = 0.0;
        double[] raw = new double[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            WorkloadEntry e = entries.get(i);
            states[i] = new EntryState(e, seed + i);
            raw[i] = e.ratio();
            sum += e.ratio();
        }
        this.cumulative = new double[entries.size()];
        double acc = 0.0;
        for (int i = 0; i < entries.size(); i++) {
            acc += raw[i] / sum;
            cumulative[i] = acc;
        }
        // Guard against floating-point drift on the last bucket.
        cumulative[cumulative.length - 1] = 1.0;

        // NEW: Check for file-based entries and load file
        int fileEntryIdx = -1;
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).usesDurationFile()) {
                fileEntryIdx = i;
                break;
            }
        }

        if (fileEntryIdx >= 0) {
            if (entries.size() > 1) {
                throw new IllegalArgumentException(
                        "Only one workload entry may use durationFile");
            }
            if (injectedFileSource != null) {
                this.fileSource = injectedFileSource;
            } else {
                WorkloadEntry fileEntry = entries.get(fileEntryIdx);
                try {
                    // Resolve the durationFile path relative to configDir if provided
                    String durationFilePath = fileEntry.durationFile();
                    Path resolvedPath = resolveDurationFilePath(durationFilePath);
                    this.fileSource = new DurationFileTaskSource(resolvedPath);
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(
                            "Failed to load duration file '" + fileEntry.durationFile() + "': " + ex.getMessage(), ex);
                }
            }
        } else {
            if (injectedFileSource != null) {
                throw new IllegalArgumentException(
                        "Injected duration source requires a durationFile workload entry");
            }
            this.fileSource = null;
        }
    }


    public Task nextTask(long taskId, boolean measurement, Runnable onComplete) {
        return nextTask(taskId, measurement, onComplete, null);
    }

    public Task nextTask(long taskId,
                         boolean measurement,
                         Runnable onComplete,
                         Task.CompletionObserver completionObserver) {
        long taskSeed = (seedMode == WorkloadSeedMode.MIXED_TASK_ID)
                ? (seed ^ Hashing.mix64(taskId ^ workloadSeed))
                : (seed + taskId);
        EntryState es = selectEntry(taskId);
        Workload w = createWorkload(es, taskSeed);
        long createdNanos = System.nanoTime();
        Task t = new Task(taskId,
                es.entry.kind(),
                es.entry.targetMillis(),
                createdNanos,
                measurement,
                w,
                onComplete,
                completionObserver);
        if (imbalance != null) {
            t.setRoutingKey(pickImbalancedRoutingKey());
        }
        return t;
    }

    /**
     * Selects a routing key for the current task.
     *
     * <p><b>Planned only.</b> {@link #planImbalanceSchedule(long)} must
     * be called before task generation begins; each call consumes one
     * entry from the pre-shuffled shard sequence. Requesting more (or
     * fewer) tasks than were planned is a programming error and fails
     * fast rather than silently changing scheduling behaviour.
     *
     * <p><b>Hot-path guarantees.</b> No locks, no atomics, no RNG, no
     * hashing. The path is:
     * <pre>
     *   target      = plannedShardSeq[plannedIndex++]
     *   routingKey  = representativeKeyByShard[target]
     * </pre>
     *
     * <p><b>Thread-safety.</b> The benchmark uses a single submitting
     * thread; this method is not synchronized. Do not call it from
     * multiple threads concurrently.
     *
     * <p><b>Determinism.</b> Same {@link ShardImbalanceConfig#randomSeed()}
     * ⇒ identical shard sequence. Different seeds ⇒ identical aggregate
     * counts, different temporal ordering.
     *
     * @throws IllegalStateException if the schedule has not been
     *         planned, or if it has been exhausted.
     */
    public long pickImbalancedRoutingKey() {
        if (plannedShardSeq == null) {
            throw new IllegalStateException(
                    "Experiment 2 requires planImbalanceSchedule() before task generation");
        }
        if (plannedIndex >= plannedShardSeq.length) {
            throw new IllegalStateException(
                    "Planned shard schedule exhausted (planned="
                            + plannedShardSeq.length + ")");
        }
        int target = plannedShardSeq[plannedIndex++];
        return representativeKeyByShard[target];
    }


    /**
     * Builds a deterministic per-shard assignment plan for a phase of
     * exactly {@code totalTasks} tasks and shuffles it with a
     * reproducible seed derived from
     * {@link ShardImbalanceConfig#randomSeed()}.
     *
     * <p><b>Allocation.</b> Uses the general largest-remainder
     * (Hamilton) method uniformly across all N shards:
     * <pre>
     *   expected[i] = M * p[i]
     *   count[i]    = floor(expected[i])
     *   remaining   = M - sum(count[i])
     *   distribute `remaining` extra tasks to shards with the largest
     *   fractional remainders (ties broken by shard index for
     *   reproducibility).
     * </pre>
     * This guarantees that integer task counts are the closest possible
     * realization of the configured probabilities:
     * <pre>
     *   p_hot   = alpha + (1 - alpha) / N
     *   p_other = (1 - alpha) / N
     * </pre>
     * At {@code alpha == 0} the split is as balanced as integer
     * arithmetic permits; at {@code alpha == 1} all M tasks target
     * exactly {@code hotShardId}.
     *
     * <p><b>Thread-safety.</b> Called between phases from the
     * benchmark's single control thread. Not synchronized.
     * Calling this method resets the planned index.
     */
    public void planImbalanceSchedule(long totalTasks) {
        if (imbalance == null || totalTasks <= 0) {
            this.plannedShardSeq = null;
            this.plannedIndex    = 0;
            return;
        }
        if (totalTasks > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "planImbalanceSchedule: totalTasks > Integer.MAX_VALUE not supported");
        }
        final int M = (int) totalTasks;
        final int N = shardCount;
        final double alpha = imbalance.alpha();
        final int hot = imbalance.hotShardId();
        final double pOther = (1.0 - alpha) / N;
        final double pHot   = pOther + alpha;

        // General largest-remainder allocation.
        int[] counts = new int[N];
        double[] rem = new double[N];
        int assigned = 0;
        for (int i = 0; i < N; i++) {
            double p = (i == hot) ? pHot : pOther;
            double expected = M * p;
            int floor = (int) Math.floor(expected);
            counts[i] = floor;
            rem[i] = expected - floor;
            assigned += floor;
        }
        int leftover = M - assigned;
        // Distribute `leftover` to the shards with the largest remainders.
        // Ties broken by shard index (stable, reproducible).
        while (leftover > 0) {
            int argmax = -1;
            double best = -1.0;
            for (int i = 0; i < N; i++) {
                if (rem[i] > best) { best = rem[i]; argmax = i; }
            }
            counts[argmax]++;
            rem[argmax] = -1.0; // consumed
            leftover--;
        }
        // Guard: at alpha=1 all mass on hot; at alpha=0 as-balanced as possible.
        // (Both handled naturally by the loop above.)

        int[] seq = new int[M];
        int p = 0;
        for (int s = 0; s < N; s++) {
            int c = counts[s];
            for (int j = 0; j < c; j++) seq[p++] = s;
        }
        // Fisher-Yates shuffle with a dedicated RNG seeded from the
        // configured randomSeed so results are reproducible.
        java.util.SplittableRandom shuffleRng =
                new java.util.SplittableRandom(imbalance.randomSeed() ^ 0xA5A5_5A5A_C3C3_3C3CL);
        for (int i = M - 1; i > 0; i--) {
            int j = shuffleRng.nextInt(i + 1);
            int tmp = seq[i]; seq[i] = seq[j]; seq[j] = tmp;
        }
        this.plannedShardSeq = seq;
        this.plannedIndex    = 0;
    }

    /** For tests: exposes the planned shard sequence (or {@code null}). */
    public int[] plannedShardSequenceForTest() {
        return plannedShardSeq == null ? null : plannedShardSeq.clone();
    }

    /** For tests/diagnostics: the representative key for each shard. */
    public long[] representativeKeysForTest() {
        return representativeKeyByShard == null ? null : representativeKeyByShard.clone();
    }

    /**
     * Finds one deterministic representative routing key per shard by
     * scanning sequential integers through the production
     * {@link Hashing#shardOf} function. Guarantees:
     * <pre>
     *   Hashing.shardOf(representativeKeyByShard[s], N, routing) == s
     * </pre>
     * Executed once at construction so the per-task path never hashes.
     */
    private static long[] buildRepresentativeKeys(int n, ShardedRoutingConfig routing) {
        long[] keys = new long[n];
        boolean[] found = new boolean[n];
        int filled = 0;
        final long maxScan = Math.max((long) n * 1024L, 1L << 16);
        for (long k = 0; k < maxScan && filled < n; k++) {
            int s = Hashing.shardOf(k, n, routing);
            if (!found[s]) {
                found[s] = true;
                keys[s] = k;
                filled++;
            }
        }
        if (filled < n) {
            for (int i = 0; i < n; i++) {
                if (!found[i]) {
                    throw new IllegalStateException(
                            "Could not find any routing key hashing to shard " + i
                                    + " after scanning " + maxScan + " candidates");
                }
            }
        }
        return keys;
    }

    private Workload createWorkload(EntryState es, long taskSeed) {
        // NEW: File mode detection - check if this entry uses duration file
        if (fileSource != null && es.entry.usesDurationFile()) {
            return switch (es.entry.kind()) {
                case CPU    -> new FileCpuBoundWorkload(fileSource, taskSeed, 8);
                case MEMORY -> new FileMemoryBoundWorkload(
                        fileSource,
                        es.memoryBuffer,
                        es.memoryPattern,
                        taskSeed,
                        es.memoryWriteBack,
                        8);
            };
        }
        throw new IllegalStateException("Only durationFile workloads are supported");
    }

    /**
     * Public test helper: creates a workload for the specified entry index.
     * Used by tests to verify workload creation without task generation.
     * 
     * @param entryIndex index into the workload entries list
     * @return the created Workload
     */
    public Workload createWorkload(int entryIndex) {
        if (entryIndex < 0 || entryIndex >= states.length) {
            throw new IndexOutOfBoundsException(
                    "Entry index " + entryIndex + " out of range [0," + states.length + ")");
        }
        EntryState es = states[entryIndex];
        long taskSeed = seed + entryIndex;
        return createWorkload(es, taskSeed);
    }

    // NEW: Public helpers for file mode
    public boolean isFileModeActive() {
        return fileSource != null;
    }

    public int fileModeTaskCount() {
        return fileSource == null ? 0 : fileSource.totalTasks();
    }

    /**
     * Returns the effective task count for time-based or fixed-count runs.
     * durationFile mode does not impose its own task limit; the caller
     * should use the configured global taskCount (often 0 for time-based runs).
     */
    public int getEffectiveTaskCount(int globalTaskCount) {
        return globalTaskCount;
    }

    private EntryState selectEntry(long taskId) {
        if (states.length == 1) return states[0];
        double u = drawUnit(taskId);
        for (int i = 0; i < cumulative.length; i++) {
            if (u < cumulative[i]) return states[i];
        }
        return states[states.length - 1];
    }

    /** Deterministic uniform [0,1) draw. */
    private double drawUnit(long taskId) {
        long z = seed ^ (taskId * 0x9E3779B97F4A7C15L);
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z = z ^ (z >>> 31);
        // Take top 53 bits, divide into [0,1).
        long bits = z >>> 11;
        return bits / (double) (1L << 53);
    }

    public WorkloadConfig workload() { return workload; }

    /**
     * Returns one {@link Calibration} per workload entry, in declared
     * order. Computed at construction time, so this is a cheap accessor.
     */
    public List<Calibration> calibrations() {
        List<Calibration> out = new ArrayList<>(states.length);
        for (EntryState s : states) {
            int bufferMB = s.memoryBuffer == null
                    ? 0
                    : (int) ((long) s.memoryBuffer.length * Long.BYTES / (1L << 20));
            out.add(new Calibration(
                    s.entry.displayName(),
                    s.entry.kind(),
                    s.entry.targetMillis(),
                    s.cpuIterations,
                    s.entry.usesFixedCpuIterations(),
                    s.cpuTargetMicros,
                    s.cpuMode,
                    s.memorySteps,
                    bufferMB,
                    s.memoryPattern,
                    s.memoryWriteBack,
                    s.entry.usesFixedMemorySteps(),
                    s.memoryTargetMicros,
                    s.memoryMode));
        }
        return out;
    }

    /**
     * Resolve a durationFile path relative to configDir if provided and the path is relative.
     * If configDir is null or the path is absolute, return the path as-is.
     */
    private Path resolveDurationFilePath(String durationFilePath) {
        Path path = Paths.get(durationFilePath);
        // If path is absolute or configDir is null, use the path as-is
        if (path.isAbsolute() || configDir == null) {
            return path;
        }
        // Otherwise, resolve relative to configDir
        return configDir.resolve(path);
    }

    /**
     * Enable HybridCPUWorkload for precise timing in Hybrid executor.
     * When true, duration-controlled CPU workloads use HybridCPUWorkload
     * instead of CpuBoundWorkloadDuration.
     */
    public void setUseHybridWorkloads(boolean enable) {
        this.useHybridWorkloads = enable;
    }
}
