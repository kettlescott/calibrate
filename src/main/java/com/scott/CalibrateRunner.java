package com.scott;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class CalibrateRunner {

    private static final int DURATION_COUNT = 200_000;
    private static final double CV_EPS = 1e-12;
    private static final int BINARY_MAX_ITERS = 50;

    public void run(CalibrateConfig config) throws InterruptedException {
        GlobalConfig global = config.global();
        Map<String, List<Double>> summary = new LinkedHashMap<>();
        System.out.printf("[calibrate] start workloads=%d workers=%d maxInflight=%d warmup=%ds measurement=%ds%n",
                config.calibrate().size(),
                global.workerCount(),
                global.maxInflight(),
                global.warmupSeconds(),
                global.measurementSeconds());
        System.out.printf("[calibrate] binarySearch repeats=%d epsilon=%.1e maxIters=%d%n",
                config.binarySearchRepeats(), config.binaryCvEps(), BINARY_MAX_ITERS);
        for (Map.Entry<String, List<CalibrateEntry>> workloadEntry : config.calibrate().entrySet()) {
            String workloadName = workloadEntry.getKey();
            List<Double> workloadThresholds = new ArrayList<>();
            for (CalibrateEntry entry : workloadEntry.getValue()) {
                workloadThresholds.add(runOneEntry(
                        global,
                        config.binarySearchRepeats(),
                        config.binaryCvEps(),
                        workloadName,
                        entry));
            }
            summary.put(workloadName, workloadThresholds);
        }
        printSummary(summary);
        System.out.println("[calibrate] finished all workloads");
    }

    private Double runOneEntry(GlobalConfig global,
                               int repeats,
                               double binaryCvEps,
                               String workloadName,
                               CalibrateEntry entry) throws InterruptedException {
        List<Double> shardedBetterCvs = new ArrayList<>();
        Double stopThresholdCv = null;
        Double hitCv = null;
        int round = 0;
        double previousCv = Double.NaN;
        WorkloadConfig workloadConfig = singleEntryWorkload(workloadName, entry);
        System.out.printf("[calibrate] workload=%s kind=%s distribution=%s durationUs=%d cv=[%.11f..%.11f]%n",
                workloadName,
                entry.kind(),
                entry.distribution(),
                entry.durationMicros(),
                entry.startCv(),
                entry.maxCv());

        for (double cv = entry.startCv(); cv <= entry.maxCv() + CV_EPS; cv *= 2.0, round++) {
            ComparisonResult coarse = compareAtCv(
                    global,
                    workloadConfig,
                    workloadName,
                    entry,
                    cv,
                    1,
                    global.seed() + round,
                    "coarse");

            System.out.printf("[calibrate] workload=%s kind=%s cv=%.11f sharded.endToEndMs.p99=%.3f shared.endToEndMs.p99=%.3f%n",
                    workloadName,
                    entry.kind(),
                    cv,
                    coarse.shardedP99Ms(),
                    coarse.sharedP99Ms());

            if (shouldStop(coarse.shardedP99Ms(), coarse.sharedP99Ms())) {
                hitCv = cv;
                break;
            }
            previousCv = cv;
        }

        // If coarse 2^n scan did not find a hit, force one run at max CV.
        if (hitCv == null && (Double.isNaN(previousCv) || previousCv + CV_EPS < entry.maxCv())) {
            double cv = entry.maxCv();
            ComparisonResult coarseAtMax = compareAtCv(
                    global,
                    workloadConfig,
                    workloadName,
                    entry,
                    cv,
                    1,
                    global.seed() + round,
                    "coarse-max");

            System.out.printf("[calibrate] workload=%s kind=%s cv=%.11f sharded.endToEndMs.p99=%.3f shared.endToEndMs.p99=%.3f%n",
                    workloadName,
                    entry.kind(),
                    cv,
                    coarseAtMax.shardedP99Ms(),
                    coarseAtMax.sharedP99Ms());

            if (shouldStop(coarseAtMax.shardedP99Ms(), coarseAtMax.sharedP99Ms())) {
                hitCv = cv;
            }
        }

        if (hitCv != null) {
            double stopCv = hitCv;
            if (!Double.isNaN(previousCv) && previousCv < hitCv) {
                System.out.printf("[calibrate] workload=%s stage=binary-search start lo=%.11f hi=%.11f repeats=%d%n",
                        workloadName, previousCv, hitCv, repeats);
                stopCv = refineStopCvBinary(
                        global,
                        workloadConfig,
                        workloadName,
                        entry,
                        previousCv,
                        hitCv,
                        repeats,
                        binaryCvEps,
                        global.seed() + 1_000_000L + round * 1_000L);
                System.out.printf("[calibrate] workload=%s stage=binary-search done thresholdCv=%.7f%n",
                        workloadName, stopCv);
            }
            shardedBetterCvs.add(stopCv);
            stopThresholdCv = stopCv;
            System.out.printf("[calibrate] workload=%s cv=%.11f stage=stop reason=sharded_gt_shared%n",
                    workloadName, hitCv);
        }

        System.out.printf("[calibrate] workload=%s sharded>shared cvs=%s%n",
                workloadName,
                shardedBetterCvs);
        return stopThresholdCv;
    }

    static boolean shouldStop(double shardedP99EndToEndMs, double sharedP99EndToEndMs) {
        return shardedP99EndToEndMs > sharedP99EndToEndMs;
    }

    private static double median(List<Double> values) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values must not be empty");
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        int mid = n / 2;
        if ((n & 1) == 1) {
            return sorted.get(mid);
        }
        return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }

    private ComparisonResult compareAtCv(GlobalConfig global,
                                         WorkloadConfig workloadConfig,
                                         String workloadName,
                                         CalibrateEntry entry,
                                         double cv,
                                         int repeats,
                                         long seedBase,
                                         String stage) throws InterruptedException {
        List<Double> shardedP99s = new ArrayList<>(repeats);
        List<Double> sharedP99s = new ArrayList<>(repeats);
        for (int repeat = 0; repeat < repeats; repeat++) {
            long seed = seedBase + repeat;
            if (repeats > 1) {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=generate-durations-%s count=%d repeat=%d/%d%n",
                        workloadName, cv, stage, DURATION_COUNT, repeat + 1, repeats);
            } else {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=generate-durations count=%d%n",
                        workloadName, cv, DURATION_COUNT);
            }

            List<Long> durations = WorkloadGenerator.generateMicros(
                    entry.distribution(), entry.durationMicros(), cv, DURATION_COUNT, seed);
            DurationFileTaskSource source = new DurationFileTaskSource(durations);

            if (repeats > 1) {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=run mode=SHARDED %s repeat=%d/%d%n",
                        workloadName, cv, stage, repeat + 1, repeats);
            } else {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=run mode=SHARDED%n", workloadName, cv);
            }
            RunResult sharded = runMode(global, workloadConfig, source, BenchmarkMode.SHARDED);

            if (repeats > 1) {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=run mode=SHARED %s repeat=%d/%d%n",
                        workloadName, cv, stage, repeat + 1, repeats);
            } else {
                System.out.printf("[calibrate] workload=%s cv=%.11f stage=run mode=SHARED%n", workloadName, cv);
            }
            RunResult shared = runMode(global, workloadConfig, source, BenchmarkMode.SHARED);

            shardedP99s.add(sharded.p99EndToEndMs());
            sharedP99s.add(shared.p99EndToEndMs());
        }
        return new ComparisonResult(median(shardedP99s), median(sharedP99s));
    }

    private double refineStopCvBinary(GlobalConfig global,
                                      WorkloadConfig workloadConfig,
                                      String workloadName,
                                      CalibrateEntry entry,
                                      double lo,
                                      double hi,
                                      int repeats,
                                      double binaryCvEps,
                                      long seedBase) throws InterruptedException {
        int iter = 0;
        while ((hi - lo) > binaryCvEps && iter < BINARY_MAX_ITERS) {
            double mid = (lo + hi) / 2.0;
            ComparisonResult midResult = compareAtCv(
                    global,
                    workloadConfig,
                    workloadName,
                    entry,
                    mid,
                    repeats,
                    seedBase + (long) iter * Math.max(1, repeats),
                    "binary");

            boolean shardedGtShared = shouldStop(midResult.shardedP99Ms(), midResult.sharedP99Ms());
            System.out.printf("[calibrate] workload=%s stage=binary-search iter=%d lo=%.7f hi=%.7f mid=%.7f sharded.p99=%.3f shared.p99=%.3f decision=%s%n",
                    workloadName,
                    iter + 1,
                    lo,
                    hi,
                    mid,
                    midResult.shardedP99Ms(),
                    midResult.sharedP99Ms(),
                    shardedGtShared ? "left" : "right");

            if (shardedGtShared) {
                hi = mid;
            } else {
                lo = mid;
            }
            iter++;
        }
        return hi;
    }

    private static void printSummary(Map<String, List<Double>> summary) {
        System.out.println("[calibrate] summary start");
        for (Map.Entry<String, List<Double>> e : summary.entrySet()) {
            String workload = e.getKey();
            List<Double> thresholds = e.getValue();
            if (thresholds == null || thresholds.isEmpty()) {
                System.out.printf("[calibrate] summary workload=%s sharded_gt_shared_cv=n/a%n", workload);
                continue;
            }
            for (int i = 0; i < thresholds.size(); i++) {
                String suffix = thresholds.size() > 1 ? "#" + i : "";
                Double cv = thresholds.get(i);
                if (cv == null) {
                    System.out.printf("[calibrate] summary workload=%s%s sharded_gt_shared_cv=n/a%n", workload, suffix);
                } else {
                    System.out.printf("[calibrate] summary workload=%s%s sharded_gt_shared_cv=%.7f%n", workload, suffix, cv);
                }
            }
        }
        System.out.println("[calibrate] summary end");
    }

    private WorkloadConfig singleEntryWorkload(String workloadName, CalibrateEntry entry) {
        WorkloadEntry w = new WorkloadEntry(
                workloadName,
                entry.kind(),
                0L,
                1.0,
                null,
                0,
                0,
                0L,
                "__generated__");
        return new WorkloadConfig(List.of(w));
    }

    private RunResult runMode(GlobalConfig global,
                              WorkloadConfig workload,
                              DurationFileTaskSource source,
                              BenchmarkMode mode) throws InterruptedException {
        long modeStart = System.nanoTime();
        Dispatcher dispatcher = createDispatcher(mode, global.workerCount());
        try {
            TaskGenerator generator = new TaskGenerator(
                    workload,
                    global.seed(),
                    global.workloadSeedMode(),
                    global.workloadSeed(),
                    null,
                    source);

            long taskId = 0L;
            if (global.warmupSeconds() > 0) {
                System.out.printf("[calibrate] mode=%s stage=warmup start seconds=%d%n", mode, global.warmupSeconds());
                PhaseResult warmup = runPhase(dispatcher,
                        generator,
                        global.maxInflight(),
                        global.warmupSeconds() * 1_000_000_000L,
                        false,
                        global.taskCount(),
                        taskId);
                taskId = warmup.nextTaskId();
                System.out.printf("[calibrate] mode=%s stage=warmup done submitted=%d%n",
                        mode, warmup.submitted());
            }

            System.out.printf("[calibrate] mode=%s stage=measurement start seconds=%d%n",
                    mode, global.measurementSeconds());
            PhaseResult measurement = runPhase(dispatcher,
                    generator,
                    global.maxInflight(),
                    global.measurementSeconds() * 1_000_000_000L,
                    true,
                    global.taskCount(),
                    taskId);
            System.out.printf("[calibrate] mode=%s stage=measurement done submitted=%d%n",
                    mode, measurement.submitted());

            PerKindLatencyRecorder recorder = new PerKindLatencyRecorder(Math.max(1, measurement.tasks().size()));
            for (Task t : measurement.tasks()) {
                recorder.record(t);
            }
            double p99Ms = recorder.overall().p99EndToEnd() / 1_000_000.0;
            return new RunResult(p99Ms, measurement.submitted());
        } finally {
            dispatcher.shutdown();
            boolean terminated = dispatcher.awaitTermination(60, TimeUnit.SECONDS);
            if (!terminated) {
                throw new IllegalStateException("Dispatcher did not terminate within timeout");
            }
            assertQueueEmpty(dispatcher);
            double elapsedSec = (System.nanoTime() - modeStart) / 1_000_000_000.0;
            System.out.printf("[calibrate] mode=%s stage=cleanup done elapsed=%.3fs%n", mode, elapsedSec);
        }
    }

    private static Dispatcher createDispatcher(BenchmarkMode mode, int workerCount) {
        return switch (mode) {
            case SHARDED -> new ShardedOnlyDispatcher(workerCount, null, ShardedRoutingConfig.defaults());
            case SHARED -> new SharedOnlyDispatcher(workerCount, null, null, 1);
            default -> throw new IllegalArgumentException("Unsupported mode for calibrate: " + mode);
        };
    }

    private static void assertQueueEmpty(Dispatcher dispatcher) {
        int queued = dispatcher.totalQueueSize();
        if (queued > 0) {
            throw new IllegalStateException("Queue still has tasks after run: " + queued + " for " + dispatcher.label());
        }
    }

    private PhaseResult runPhase(Dispatcher dispatcher,
                                 TaskGenerator generator,
                                 int maxInflight,
                                 long durationNanos,
                                 boolean measurement,
                                 int taskLimit,
                                 long startTaskId) throws InterruptedException {
        Semaphore permits = new Semaphore(maxInflight);
        Runnable releasePermit = permits::release;

        long submitStart = System.nanoTime();
        long deadline = submitStart + durationNanos;
        long taskId = startTaskId;
        int submitted = 0;

        List<Task> tasks = measurement ? new ArrayList<>() : List.of();

        while ((taskLimit > 0 ? submitted < taskLimit : System.nanoTime() < deadline)) {
            permits.acquire();
            Task task = generator.nextTask(taskId, measurement, releasePermit);
            dispatcher.submit(task);
            if (measurement) {
                tasks.add(task);
            }
            taskId++;
            submitted++;
        }

        permits.acquire(maxInflight);
        permits.release(maxInflight);
        return new PhaseResult(tasks, submitted, taskId);
    }

    private record PhaseResult(List<Task> tasks, int submitted, long nextTaskId) {
    }

    private record RunResult(double p99EndToEndMs, int submitted) {
    }

    private record ComparisonResult(double shardedP99Ms, double sharedP99Ms) {
    }
}

