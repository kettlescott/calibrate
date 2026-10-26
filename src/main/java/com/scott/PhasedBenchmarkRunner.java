package com.scott;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;

/**
 * Runner for phased benchmark workloads.
 *
 * <p>Orchestrates multi-phase execution with:
 * <ul>
 *   <li>File preloading (one-time, at startup)</li>
 *   <li>Cyclic task submission by phase</li>
 *   <li>Per-phase measurement and statistics</li>
 * </ul>
 */
public final class PhasedBenchmarkRunner {

    private final PhasedWorkloadEntry entry;
    private final BenchmarkExecutor executor;
    private final Path baseDir;
    private final int maxInflight;

    private Map<String, long[]> fileCache;  // durationFile -> array of micros
    private List<Integer> fileIndices;       // current index per phase
    private PhasedLatencyCollector latencyCollector;
    private Predicate<Task> measurementGate = task -> true;

    public PhasedBenchmarkRunner(PhasedWorkloadEntry entry,
                                 BenchmarkExecutor executor,
                                 Path baseDir) {
        this(entry, executor, baseDir, Integer.MAX_VALUE);
    }

    public PhasedBenchmarkRunner(PhasedWorkloadEntry entry,
                                 BenchmarkExecutor executor,
                                 Path baseDir,
                                 int maxInflight) {
        this.entry = entry;
        this.executor = executor;
        this.baseDir = baseDir;
        this.maxInflight = maxInflight > 0 ? maxInflight : Integer.MAX_VALUE;
    }

    /**
     * Preloads all unique duration files into memory.
     * Called once at startup.
     */
    public void preloadDurationFiles() throws Exception {
        fileCache = new java.util.LinkedHashMap<>();
        for (PhasedConfig phase : entry.phases()) {
            String file = phase.durationFile();
            if (!fileCache.containsKey(file)) {
                Path resolved = baseDir == null ? Path.of(file) : baseDir.resolve(file).normalize();
                DurationFileTaskSource source = new DurationFileTaskSource(resolved);
                long[] durations = new long[source.totalTasks()];
                for (int i = 0; i < durations.length; i++) {
                    durations[i] = source.nextDurationMicros();
                }
                fileCache.put(file, durations);
            }
        }
    }

    /**
     * Returns the file cache (for testing purposes).
     */
    public Map<String, long[]> getFileCache() {
        return fileCache;
    }

    /**
     * Runs the complete multi-phase benchmark.
     * Returns per-phase statistics.
     */
    public List<PhaseStatistics> run() throws Exception {
        if (fileCache == null) {
            throw new IllegalStateException("Must call preloadDurationFiles() first");
        }

        if (executor instanceof AdaptiveHybridExecutor ahe) {
            ahe.markMeasurementStart();
            measurementGate = ahe::shouldRecordStableMeasurement;
        } else {
            measurementGate = task -> true;
        }

        long measurementStartNanos = System.nanoTime();
        List<PhaseStatistics> phaseStats = new ArrayList<>();
        latencyCollector = new PhasedLatencyCollector(entry.phases().size(), measurementGate);
        fileIndices = new ArrayList<>();
        for (int i = 0; i < entry.phases().size(); i++) {
            fileIndices.add(0);
        }

        for (int phaseId = 0; phaseId < entry.phases().size(); phaseId++) {
            PhasedConfig phase = entry.phases().get(phaseId);
            long phaseDurationNanos = phase.durationSeconds() * 1_000_000_000L;
            long phaseStartNanos = System.nanoTime();

            PhaseStatistics stats = runPhase(phaseId, phase, phaseDurationNanos, measurementStartNanos);
            phaseStats.add(stats);

            long phaseElapsedNanos = System.nanoTime() - phaseStartNanos;
            System.out.printf("[Phase %d] Completed in %.3f seconds, %d tasks submitted%n",
                    phaseId, phaseElapsedNanos / 1_000_000_000.0, stats.tasksSubmitted);
        }

        return phaseStats;
    }

    /**
     * Runs a single phase.
     */
    private PhaseStatistics runPhase(int phaseId, PhasedConfig phase,
                                     long phaseDurationNanos, long measurementStartNanos) throws Exception {
        long phaseStartNanos = System.nanoTime();
        long phaseDeadline = phaseStartNanos + phaseDurationNanos;
        Semaphore permits = new Semaphore(maxInflight);
        Runnable releasePermit = permits::release;

        long taskId = 0;
        int submitted = 0;
        int fileIndex = fileIndices.get(phaseId);
        long[] durations = fileCache.get(phase.durationFile());

        // Allocate the memory buffer ONCE per phase and share it across all
        // tasks/workers. The workload is read-only (execute() only reads
        // buf[idx]) and runs for a fixed target duration regardless of buffer
        // content, so a single shared buffer is safe and keeps the producer
        // thread from re-allocating 8MB per task (the former throughput killer).
        long[] memBuffer = null;
        if (entry.kind() == WorkloadKind.MEMORY) {
            memBuffer = allocateBuffer(entry.memoryOrDefaults().bufferMB(), 0x9E3779B97F4A7C15L);
        }

        while (System.nanoTime() < phaseDeadline) {
            if (!permits.tryAcquire()) {
                permits.acquire();
            }

            long durationMicros = durations[fileIndex];
            fileIndex = (fileIndex + 1) % durations.length;

            Workload workload = createWorkload(durationMicros, memBuffer);
            Task task = new Task(
                    taskId++,
                    entry.kind(),
                    durationMicros / 1000,
                    System.nanoTime(),
                    true,
                    workload,
                    releasePermit,
                    latencyCollector);
            task.setPhaseId(phaseId);

            executor.submit(task);
            submitted++;
        }

        permits.acquire(maxInflight);
        permits.release(maxInflight);

        fileIndices.set(phaseId, fileIndex);

        long phaseElapsedNanos = System.nanoTime() - phaseStartNanos;
        return new PhaseStatistics(
                phaseId,
                submitted,
                phaseElapsedNanos,
                (System.nanoTime() - measurementStartNanos) / 1_000_000_000.0);
    }

    /**
     * Creates a workload for the given duration.
     * Supports CPU and MEMORY workloads with duration control.
     * Uses specialized Hybrid variants when executor is AdaptiveHybridExecutor.
     */
    private Workload createWorkload(long durationMicros, long[] sharedBuffer) {
        long seed = System.nanoTime() ^ durationMicros;
        
        // Use consistent batch size for both CPU and Memory for fair comparison
        // Every 64 operations, check elapsed time to balance precision and overhead
        final int BATCH_SIZE = 64;
        
        switch (entry.kind()) {
            case CPU:
                if (executor instanceof AdaptiveHybridExecutor) {
                    return new HybridCPUWorkload(durationMicros, seed);
                }
                return new CpuBoundWorkloadDuration(durationMicros, seed, BATCH_SIZE);
            
            case MEMORY:
                MemoryWorkloadConfig memConfig = entry.memoryOrDefaults();
                // Reuse the per-phase shared buffer (allocated once in runPhase).
                long[] buffer = sharedBuffer;
                
                if (executor instanceof AdaptiveHybridExecutor) {
                    return new HybridMemoryBoundWorkload(
                        buffer, durationMicros, memConfig.accessPattern(), 
                        seed, memConfig.writeBack());
                }
                return new MemoryBoundWorkloadDuration(
                    buffer, durationMicros, memConfig.accessPattern(), 
                    seed, memConfig.writeBack(), BATCH_SIZE);
            
            default:
                throw new IllegalArgumentException("Unsupported workload kind: " + entry.kind());
        }
    }
    
    /**
     * Allocates a memory buffer for MEMORY workload.
     */
    private long[] allocateBuffer(int bufferMB, long seed) {
        int longs = (bufferMB * 1024 * 1024) / 8;
        long[] buf = new long[longs];
        long x = seed == 0L ? 0x9E3779B97F4A7C15L : seed;
        for (int i = 0; i < longs; i++) {
            x ^= (x << 13); x ^= (x >>> 7); x ^= (x << 17);
            buf[i] = x;
        }
        return buf;
    }


    /**
     * Per-phase statistics record.
     */
    public static final class PhaseStatistics {
        public final int phaseId;
        public final int tasksSubmitted;
        public final long elapsedNanos;
        public final double measurementTimeSecs;

        public PhaseStatistics(int phaseId, int tasksSubmitted, long elapsedNanos, double measurementTimeSecs) {
            this.phaseId = phaseId;
            this.tasksSubmitted = tasksSubmitted;
            this.elapsedNanos = elapsedNanos;
            this.measurementTimeSecs = measurementTimeSecs;
        }

        @Override
        public String toString() {
            return String.format("Phase[%d]: %d tasks in %.3fs (%.1f tasks/sec)",
                    phaseId, tasksSubmitted, elapsedNanos / 1_000_000_000.0,
                    tasksSubmitted / (elapsedNanos / 1_000_000_000.0));
        }
    }

    private static final class PhasedLatencyCollector implements Task.CompletionObserver {
        private final LatencyRecorder overall = new LatencyRecorder();
        private final List<LatencyRecorder> perPhase = new ArrayList<>();
        private final Predicate<Task> measurementGate;

        PhasedLatencyCollector(int phaseCount, Predicate<Task> measurementGate) {
            this.measurementGate = measurementGate == null ? task -> true : measurementGate;
            for (int i = 0; i < phaseCount; i++) {
                perPhase.add(new LatencyRecorder());
            }
        }

        @Override
        public synchronized void onTaskCompleted(Task task) {
            if (!task.isMeasurement()) {
                return;
            }
            if (!measurementGate.test(task)) {
                return;
            }
            long so = task.submitOverheadNanos();
            long qw = task.queueWaitTimeNanos();
            long ex = task.executionTimeNanos();
            long e2e = task.endToEndLatencyNanos();
            overall.recordRaw(so, qw, ex, e2e);
            int phaseId = task.phaseId();
            if (phaseId >= 0 && phaseId < perPhase.size()) {
                perPhase.get(phaseId).recordRaw(so, qw, ex, e2e);
            }
        }

        LatencyRecorder overall() {
            return overall;
        }

        LatencyRecorder forPhase(int phaseId) {
            return perPhase.get(phaseId);
        }
    }
}
