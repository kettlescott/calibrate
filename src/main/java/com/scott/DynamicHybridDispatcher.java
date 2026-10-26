package com.scott;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Dynamic Hybrid dispatcher — the {@link BenchmarkMode#DYNAMIC_HYBRID}
 * executor.
 *
 * <p>Routing policy (see {@link DynamicHybridConfig}):
 * <ul>
 *   <li>{@code executionTime <= Tc}  → SHORT → round-robin over
 *       currently {@link ShardState#ACTIVE} local queues.</li>
 *   <li>{@code executionTime >  Tc}  → LONG  → single global Shared
 *       queue.</li>
 * </ul>
 *
 * <p>The dispatcher owns a fixed pool of {@code N} worker threads for
 * its entire lifetime. It never creates or destroys threads. Elastic
 * capacity is realised by moving individual workers between the
 * {@link ShardState#ACTIVE} (drain local) and {@link ShardState#INACTIVE}
 * (drain shared) domains — the total worker count remains {@code N}.
 *
 * <p>A single-threaded controller (a {@code ScheduledExecutorService}
 * whose thread is never counted as a worker) ticks every
 * {@link DynamicHybridConfig#controllerIntervalMicros()} and produces at
 * most one capacity decision per tick with hysteresis (see
 * {@link #evaluateScalingDecision}).
 *
 * <p>Not designed for concurrent submitters: consistent with the other
 * dispatchers in this project, {@link #submit(Task)} is called from a
 * single benchmark thread. The round-robin cursor and worker-state
 * fields are still atomic so the controller thread can observe them
 * safely.
 */
public final class DynamicHybridDispatcher implements Dispatcher {

    /** Sentinel returned by {@link #pickActiveShard()} when no shard is ACTIVE. */
    static final int NO_ACTIVE_SHARD = -1;

    private final DynamicHybridConfig config;
    private final int workerCount;

    /** Global (SHARED) queue for LONG tasks. */
    private final LinkedBlockingQueue<Task> sharedQueue = new LinkedBlockingQueue<>();

    /** One local queue per worker. */
    private final LinkedBlockingQueue<Task>[] localQueues;

    /** One state slot per worker. */
    private final AtomicReferenceArray<ShardState> states;

    /**
     * Per-shard admission counter used as the SHORT-vs-scale-in race
     * barrier (see {@link #submit(Task)} for the full protocol proof).
     * <ul>
     *   <li>{@code >= 0} — open, value is the number of submitters
     *       currently mid-offer on this shard.</li>
     *   <li>{@code Integer.MIN_VALUE} — closed by controller; no new
     *       admission can succeed until the {@code DRAINING -> INACTIVE}
     *       transition reopens it.</li>
     * </ul>
     * Submitters CAS {@code n -> n+1}; controller CASes {@code 0 -> MIN_VALUE}.
     * These two CASes cannot both succeed on the same starting value,
     * so no submitter can commit an offer to a shard that has already
     * been closed for scale-in.
     */
    private final AtomicIntegerArray admissionCount;

    /** Per-shard nanoTime when the current ACTIVATING request was issued. -1 = none. */
    private final AtomicLongArray activationRequestNanos;
    /** Per-shard nanoTime when the current DRAINING request was issued. -1 = none. */
    private final AtomicLongArray drainRequestNanos;

    /** Cap on ACTIVE shards: {@code workerCount - config.minSharedWorkers()}. */
    private final int maxShardedWorkers;

    private final Thread[] workerThreads;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    /** Round-robin cursor over ACTIVE shards (used by the submitter). */
    private final AtomicInteger rrCursor = new AtomicInteger(0);


    // ---- EWMA / pressure state (updated by workers, read by controller) ----

    /** Short-task service time EWMA, in nanoseconds. -1 = uninitialised. */
    private final AtomicLong shortEwmaNanos = new AtomicLong(-1L);

    /** Currently ACTIVE shard count Ns. Kept in sync with {@link #states}. */
    private final AtomicInteger activeShardCount = new AtomicInteger(0);

    /**
     * Snapshot of the current ACTIVE shard IDs. Rebuilt (under
     * {@link #activeSnapshotLock}) whenever the ACTIVE set changes.
     * Read once by {@link #pickActiveShard()} to implement TRUE
     * round-robin over the ACTIVE set even when ACTIVE IDs are sparse
     * (e.g. {@code ACTIVE = {3, 7, 12, 21}}).
     *
     * <p>Volatile publish/read ensures the snapshot is a coherent
     * {@code int[]}: no torn reads, no partial construction. A stale
     * snapshot is acceptable — the admission-barrier CAS on the
     * chosen shard will fail if the shard has just been closed for
     * scale-in, and the submitter re-picks (bounded retry).
     */
    private volatile int[] activeShardIds;

    /** Serialises rebuilds of {@link #activeShardIds}. Not on hot path. */
    private final Object activeSnapshotLock = new Object();

    /**
     * Reserved sharded capacity = number of workers that are NOT
     * currently available to service {@link #sharedQueue}. That is
     * every worker in {@link ShardState#ACTIVE},
     * {@link ShardState#ACTIVATING} or {@link ShardState#DRAINING}
     * (all three consume only local SHORT tasks or are transitioning
     * out of the shared domain and cannot help LONG work), plus
     * (transiently) any not-yet-committed reservation held by the
     * scale-out attempt currently executing on the controller thread.
     *
     * <p>Kept as a single counter because it is the quantity we want to
     * bound: {@code reservedShardedCapacity <= maxShardedWorkers} so at
     * least {@code minSharedWorkers} threads remain servicing LONG
     * work. {@link #activeShardCount} is a strict subset (routable
     * shards only) used as {@code Ns} in the pressure formula.
     *
     * <p>Transition accounting:
     * <ul>
     *   <li>{@code INACTIVE -> ACTIVATING}: reservation was already
     *       incremented by the scale-out attempt <b>before</b> the
     *       state CAS; on CAS failure the attempt rolls it back.</li>
     *   <li>{@code ACTIVATING -> ACTIVE}: only {@code activeShardCount}
     *       increments; reservation is unchanged (worker still
     *       reserved, just now in the ACTIVE half of the reservation).</li>
     *   <li>{@code ACTIVE -> DRAINING}: only {@code activeShardCount}
     *       decrements; reservation is unchanged (worker is still
     *       consuming its local SHORT queue, still NOT servicing
     *       {@code sharedQueue}).</li>
     *   <li>{@code DRAINING -> INACTIVE}: reservation is released.
     *       This is the sole release point for a scale-in cycle.</li>
     * </ul>
     *
     * <p>Consequence: the invariant
     * {@code workerCount - reservedShardedCapacity >= minSharedWorkers}
     * holds throughout every transition, so at least
     * {@code minSharedWorkers} threads are always available for LONG.
     */
    private final AtomicInteger reservedShardedCapacity = new AtomicInteger(0);

    // ---- Metric counters (never on the hot path) ----
    private final AtomicLong scaleOutCount = new AtomicLong(0);
    private final AtomicLong scaleInCount  = new AtomicLong(0);
    /** Diagnostic: how many SHORTs fell back to the shared queue because Ns==0. */
    private final AtomicLong shortFallbackCount = new AtomicLong(0);
    /**
     * Number of tasks whose {@link Task#run()} threw a {@link RuntimeException}.
     * Incremented from the worker loop's narrow catch block; a failed
     * task never terminates the worker thread and never contributes to
     * the SHORT EWMA.
     */
    private final AtomicLong taskFailureCount = new AtomicLong(0);

    // ---- Transition-latency metrics (P1) ----
    private final AtomicLong scaleOutLatencyCount   = new AtomicLong(0);
    private final AtomicLong scaleOutLatencyTotalNs = new AtomicLong(0);
    private final AtomicLong scaleOutLatencyMaxNs   = new AtomicLong(0);
    private final AtomicLong scaleInLatencyCount    = new AtomicLong(0);
    private final AtomicLong scaleInLatencyTotalNs  = new AtomicLong(0);
    private final AtomicLong scaleInLatencyMaxNs    = new AtomicLong(0);

    // ---- Actual controller-interval jitter metrics (P1) ----
    /** Previous tick start (controller thread only; volatile so tests can read). */
    private volatile long previousTickStartNanos = 0L;
    private final AtomicLong controllerIntervalSamples = new AtomicLong(0);
    private final AtomicLong controllerIntervalTotalNs = new AtomicLong(0);
    private final AtomicLong controllerIntervalMaxNs   = new AtomicLong(0);

    // ---- Per-worker SHORT timing (feeds EWMA on controller tick) (P1) ----
    //
    // Encoding: each slot is a packed long where
    //     high 16 bits = SHORT-completion count in this tick window
    //     low  48 bits = cumulative SHORT execution time (nanoseconds)
    //
    // Why 16 / 48 and NOT the previous 32 / 32 (µs)?
    //   - 32-bit MICROSECOND sum could overflow within a long tick and
    //     the carry would corrupt the count field. Using 48-bit
    //     NANOSECOND sum makes overflow physically unreachable at any
    //     realistic tick period, and CAS-based bounds-checking removes
    //     the last theoretical carry path.
    //
    // Range bounds (documented for the paper):
    //   count = 2^16 - 1 = 65 535 samples per tick per worker.
    //     A worker doing 65k tasks per tick means a task period well
    //     under 100 ns even at a 1 µs tick — outside any realistic
    //     benchmark.
    //   sum  = 2^48 - 1 ns  = ~3.13 days per stripe per tick.
    //     Even a fully-utilised worker cannot accumulate 3 days of
    //     execution time inside one tick.
    //
    // Sample acceptance rule (fix 2): a sample is accepted ONLY if
    // BOTH would-be-new count AND would-be-new sum stay within the
    // above bounds. If either bound would be crossed, the entire
    // sample is dropped — the packed slot is left unchanged so
    // (count, sum) always describe the same accepted sample set. We
    // NEVER saturate one field while advancing the other.
    //
    // Update path uses a CAS loop instead of getAndAdd so we can
    // enforce the pair-accept-or-drop rule atomically. Since each
    // worker is the sole writer to its own slot, the CAS only ever
    // contends with the controller's rare getAndSet(0), so the loop
    // essentially always succeeds on the first try.
    private static final int  SHORT_SAMPLE_COUNT_SHIFT = 48;
    private static final long SHORT_SAMPLE_SUM_MASK    = (1L << 48) - 1L;
    private static final long SHORT_SAMPLE_COUNT_MAX   = 0xFFFFL;   // 65 535
    private final AtomicLongArray perWorkerShortSamples;

    /**
     * Test hook: if non-null, invoked by {@link #submit(Task)} after a
     * successful admission acquisition and BEFORE {@code offer()}. Used
     * only by concurrency tests to inject a controller close and prove
     * no strand-race exists. Never set in production.
     */
    volatile Runnable admissionCommittedHookForTests;

    /**
     * Test hook: if non-null, invoked by {@link #submit(Task)} AFTER
     * {@link #pickActiveShard()} returns a shard id but BEFORE the
     * admission-count CAS. Lets a concurrency test pause a submitter
     * exactly at the stale-snapshot vulnerability point. Never set in
     * production. Receives the shard id.
     */
    volatile java.util.function.IntConsumer preAdmissionHookForTests;

    /**
     * Aggregate count of SHORT tasks currently sitting in any local
     * queue. Incremented once per successful SHORT routing (in
     * {@link #submit(Task)}) and decremented once per SHORT dequeue
     * (in {@link #workerLoop(int)}). Read by the controller in O(1)
     * via {@link LongAdder#sum()} — no per-tick scan of shard queues,
     * no per-tick reads of {@link #states}.
     *
     * <p>{@link LongAdder} is chosen over {@link AtomicLong} because the
     * hot path here is many-writers / rare-reader: every submitted or
     * completed SHORT task performs one add, and only the controller
     * (once per {@code controllerIntervalMicros}) sums the stripes.
     * That's the exact workload {@code LongAdder} was designed for.
     *
     * <p><b>Semantic note wrt spec:</b> spec defines
     * {@code Qs = pending SHORTs across currently ACTIVE shards}. This
     * counter also includes SHORTs sitting in a shard that has since
     * transitioned to DRAINING (they were routed while the shard was
     * ACTIVE). This is intentional: those tasks still represent real
     * queued short work the executor must complete, and counting them
     * only slows down (never speeds up) scale-in — which is the
     * conservative direction for stability. It never causes routing
     * decisions to violate the ACTIVE-only rule (that is enforced
     * separately in {@link #pickActiveShard()}).
     */
    private final LongAdder queuedShortAdder = new LongAdder();

    // ---- Controller runtime metrics (updated by the controller thread only) ----
    private final AtomicLong controllerTickCount        = new AtomicLong(0);
    private final AtomicLong controllerTotalRuntimeNanos = new AtomicLong(0);
    private final AtomicLong controllerMaxRuntimeNanos   = new AtomicLong(0);
    /** Ticks whose runtime exceeded the configured period. */
    private final AtomicLong controllerOverrunCount      = new AtomicLong(0);

    // ---- Controller ----
    private final java.util.concurrent.ScheduledExecutorService controller;
    /**
     * Configured controller period converted to nanoseconds. Cached at
     * construction so the tick path never touches the config on the
     * hot path and can detect overruns with a single primitive compare.
     */
    private final long controllerPeriodNanos;

    /**
     * Pluggable capacity policy. Defaults to {@link HysteresisPressurePolicy}
     * (the EWMA + Ps + H/L rule from the spec). Inject a different
     * {@link CapacityPolicy} via the extended constructor for
     * experimentation or unit-testing.
     */
    private final CapacityPolicy capacityPolicy;

    @SuppressWarnings("unchecked")
    public DynamicHybridDispatcher(DynamicHybridConfig config, int workerCount) {
        this(config, workerCount, HysteresisPressurePolicy.INSTANCE);
    }

    /**
     * Extended constructor allowing a custom {@link CapacityPolicy}.
     * Existing callers that pass no policy get the default
     * {@link HysteresisPressurePolicy}, so behaviour is byte-identical.
     */
    @SuppressWarnings("unchecked")
    public DynamicHybridDispatcher(DynamicHybridConfig config, int workerCount,
                                   CapacityPolicy capacityPolicy) {
        if (config == null) {
            throw new IllegalArgumentException(
                    "DynamicHybridDispatcher requires a DynamicHybridConfig (add a 'dynamicHybrid:' YAML block).");
        }
        if (capacityPolicy == null) {
            throw new IllegalArgumentException("capacityPolicy must not be null");
        }
        config.validate(workerCount);
        this.config = config;
        this.capacityPolicy = capacityPolicy;
        this.workerCount = workerCount;
        this.localQueues = new LinkedBlockingQueue[workerCount];
        this.states = new AtomicReferenceArray<>(workerCount);
        this.admissionCount         = new AtomicIntegerArray(workerCount);
        this.activationRequestNanos = new AtomicLongArray(workerCount);
        this.drainRequestNanos      = new AtomicLongArray(workerCount);
        this.perWorkerShortSamples  = new AtomicLongArray(workerCount);
        this.maxShardedWorkers      = config.maxShardedWorkers(workerCount);
        for (int i = 0; i < workerCount; i++) {
            localQueues[i] = new LinkedBlockingQueue<>();
            // First Nmin shards start ACTIVE; the rest start INACTIVE.
            states.set(i, i < config.minShardedWorkers() ? ShardState.ACTIVE : ShardState.INACTIVE);
            // admissionCount[i] defaults to 0 = "open, no in-flight admissions"
            activationRequestNanos.set(i, -1L);
            drainRequestNanos.set(i, -1L);
        }
        this.activeShardCount.set(config.minShardedWorkers());
        this.reservedShardedCapacity.set(config.minShardedWorkers());
        rebuildActiveSnapshot();   // publish initial ACTIVE = [0..Nmin)

        this.workerThreads = new Thread[workerCount];
        for (int i = 0; i < workerCount; i++) {
            final int wid = i;
            Thread t = new Thread(() -> workerLoop(wid), "DynHybridWorker-" + i);
            t.setDaemon(false);
            workerThreads[i] = t;
        }
        for (Thread t : workerThreads) t.start();

        this.controller = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "DynHybridController");
            t.setDaemon(true);
            return t;
        });
        long periodMicros = Math.max(1L, config.controllerIntervalMicros());
        this.controllerPeriodNanos = periodMicros * 1_000L;
        controller.scheduleAtFixedRate(this::controllerTick,
                periodMicros, periodMicros, TimeUnit.MICROSECONDS);
    }

    // =================================================================
    //  Task classification + routing
    // =================================================================

    /** Classify a task as SHORT or LONG based on its predicted execution time. */
    static boolean isShort(long predictedMicros, long thresholdMicros) {
        return predictedMicros <= thresholdMicros;
    }

    /**
     * Predicted execution time for a task, in microseconds. Uses the
     * static {@code targetMillis} already carried on the task — no
     * runtime prediction is performed (per the spec's "do not build a
     * prediction mechanism" guidance).
     */
    private long predictedMicros(Task task) {
        return task.targetMillis() * 1_000L;
    }

    /**
     * True round-robin over the current ACTIVE shard set. Returns
     * {@link #NO_ACTIVE_SHARD} when the set is empty. Reads the
     * {@link #activeShardIds} snapshot once — one volatile load
     * followed by one primitive modulo + array load.
     *
     * <h4>Why a snapshot array, not "scan states from cursor"</h4>
     * <p>The previous "scan-until-ACTIVE" implementation biased routing
     * toward the lowest ACTIVE index whenever the ACTIVE set was sparse
     * (e.g. {@code ACTIVE = {1,2,...,7}} inside {@code N = 32} sent
     * many cursor positions to shard 1). With the snapshot array the
     * cursor indexes into {@code activeShardIds} directly, so each
     * ACTIVE shard receives an equal share regardless of ID sparsity.
     *
     * <p>A stale snapshot is safe: the admission-barrier CAS on the
     * chosen shard will fail if that shard has since been closed for
     * scale-in, and {@link #submit(Task)}'s bounded retry picks again.
     */
    int pickActiveShard() {
        int[] snap = activeShardIds;    // one volatile load
        int len = snap.length;
        if (len == 0) return NO_ACTIVE_SHARD;
        int start = rrCursor.getAndIncrement();
        // floorMod handles negative cursor after AtomicInteger overflow.
        return snap[Math.floorMod(start, len)];
    }

    /**
     * Rebuild {@link #activeShardIds}. Called whenever the ACTIVE set
     * changes (worker completes {@code ACTIVATING -> ACTIVE},
     * controller commits {@code ACTIVE -> DRAINING}, or test hook
     * forces a state). O(N) work under {@link #activeSnapshotLock};
     * transitions are rare compared to task-submit rate, so lock
     * contention is negligible in production.
     *
     * <p><b>Race-safe single pass.</b> State transitions do NOT acquire
     * {@link #activeSnapshotLock}, so a two-pass "count then fill"
     * approach can either overflow an exactly-sized array (ACTIVE set
     * grew between passes) or leave bogus default-{@code 0} slots
     * (ACTIVE set shrank). We instead do exactly one scan into a
     * temporary {@code int[workerCount]}, then trim with
     * {@link Arrays#copyOf(int[], int)}. Any concurrent transition
     * during the scan can still make the snapshot slightly stale, but
     * the admission-barrier CAS in {@link #submit(Task)} rejects a
     * shard that has since started scale-in — so a stale snapshot is
     * safe by construction.
     */
    private void rebuildActiveSnapshot() {
        synchronized (activeSnapshotLock) {
            int[] tmp = new int[workerCount];
            int count = 0;
            for (int i = 0; i < workerCount; i++) {
                if (states.get(i) == ShardState.ACTIVE) {
                    tmp[count++] = i;
                }
            }
            activeShardIds = Arrays.copyOf(tmp, count);
        }
    }

    @Override
    public void submit(Task task) throws InterruptedException {
        final long predUs = predictedMicros(task);
        final boolean isShort = isShort(predUs, config.crossoverThresholdMicros());
        task.markEnqueued();

        if (!isShort) {
            sharedQueue.offer(task);
            return;
        }

        // ---- SHORT admission protocol (P0 race barrier) ----
        //
        // Invariants we uphold:
        //
        //   (I1) Once the controller CAS-es admissionCount[i]: 0 -> MIN_VALUE,
        //        no admission can succeed on shard i until DRAINING -> INACTIVE
        //        reopens it (admissionCount[i] = 0 in the worker loop).
        //
        //   (I2) While our admission (n -> n+1) is held on shard i, the
        //        controller cannot start a new scale-in on i (its CAS
        //        0 -> MIN_VALUE would fail because n+1 > 0). Therefore
        //        if we observe states[i] == ACTIVE while holding admission,
        //        i STAYS ACTIVE until we release.
        //
        //   (I3) A stale activeShardIds snapshot can still name a shard
        //        that has since gone through ACTIVE -> DRAINING -> INACTIVE
        //        and been reopened for admission. Step 1 catches the
        //        common case (state != ACTIVE at CAS time); the
        //        post-admission revalidation in step 3 closes the
        //        interleaving where the full cycle completes AFTER we
        //        pick the shard but BEFORE we take admission.
        //
        // Bounded retry: at worst we pick a different ACTIVE shard on
        // each failure. rrCursor advances on every pick, so retries do
        // NOT spin on the same shard. Cap at N attempts to avoid
        // pathological loops when many shards close simultaneously.
        for (int attempt = 0; attempt < workerCount; attempt++) {
            int shard = pickActiveShard();
            if (shard == NO_ACTIVE_SHARD) break;            // fall through to fallback
            // Test hook — fires between pick and admission CAS so a
            // stale-snapshot race can be driven deterministically.
            java.util.function.IntConsumer preHook = preAdmissionHookForTests;
            if (preHook != null) preHook.accept(shard);
            // Step 1: fast-path pre-check — cheap way to skip a shard
            // that is already closed.
            int c = admissionCount.get(shard);
            if (c < 0) continue;                            // shard closed → re-pick
            // Step 2: take admission via CAS.
            if (!admissionCount.compareAndSet(shard, c, c + 1)) continue; // lost race → retry

            // Test hook — used only by concurrency tests.
            Runnable hook = admissionCommittedHookForTests;
            if (hook != null) hook.run();

            // Step 3: post-admission ACTIVE-state revalidation.
            // The snapshot we used may have been stale, and the shard
            // may have completed the full cycle
            //     ACTIVE -> DRAINING -> INACTIVE (admissions reopened)
            // between pickActiveShard() and our admissionCount CAS.
            // If states[shard] is anything other than ACTIVE right now,
            // the local queue is NOT being serviced as a SHORT queue —
            // an INACTIVE worker only polls sharedQueue — and offering
            // here would strand the task. Release admission and retry.
            if (states.get(shard) != ShardState.ACTIVE) {
                admissionCount.decrementAndGet(shard);
                continue;
            }
            // From here on, invariant I2 gives us: shard stays ACTIVE
            // until we release admission below. The offer() is safe.
            try {
                queuedShortAdder.increment();               // O(1) striped counter
                localQueues[shard].offer(task);
            } finally {
                admissionCount.decrementAndGet(shard);      // release slot
            }
            return;
        }

        // Fallback: no ACTIVE shard could admit us (Nmin==0 startup, or
        // all shards momentarily closed for scale-in). Route to shared
        // queue so the task still executes; observed via
        // shortFallbackCount. queuedShortAdder is not incremented here
        // (task is not on any local queue).
        shortFallbackCount.incrementAndGet();
        sharedQueue.offer(task);
    }

    // =================================================================
    //  Worker loop
    // =================================================================

    private void workerLoop(int wid) {
        LinkedBlockingQueue<Task> local = localQueues[wid];
        while (!shutdown.get() || !sharedQueue.isEmpty() || !local.isEmpty()) {
            ShardState s = states.get(wid);
            // ---- Safe boundary: honour a pending ACTIVATING request ----
            if (s == ShardState.ACTIVATING) {
                if (states.compareAndSet(wid, ShardState.ACTIVATING, ShardState.ACTIVE)) {
                    // reservedShardedCapacity was already incremented
                    // by requestScaleOut; do NOT double-count. Only the
                    // routable-shard counter (Ns) moves here.
                    activeShardCount.incrementAndGet();
                    rebuildActiveSnapshot();     // ACTIVE set changed
                    // P1: scale-out latency = nanoTime@request → nanoTime@ACTIVE.
                    long reqNs = activationRequestNanos.getAndSet(wid, -1L);
                    if (reqNs > 0) recordScaleOutLatency(System.nanoTime() - reqNs);
                    s = ShardState.ACTIVE;
                }
            }

            Task task;
            boolean fromLocal;
            try {
                if (s == ShardState.ACTIVE) {
                    task = local.poll(1, TimeUnit.MILLISECONDS);
                    fromLocal = (task != null);
                } else if (s == ShardState.DRAINING) {
                    task = local.poll();
                    if (task == null) {
                        // Local drained → return to shared.
                        if (states.compareAndSet(wid, ShardState.DRAINING, ShardState.INACTIVE)) {
                            // Reopen admissions so a future scale-out
                            // CAS-es INACTIVE → ACTIVATING and this
                            // shard can admit again.
                            admissionCount.set(wid, 0);
                            // Release reserved sharded capacity — the
                            // worker is now finally available to service
                            // sharedQueue/LONG work. This is the sole
                            // release point for a scale-in cycle.
                            reservedShardedCapacity.decrementAndGet();
                            long reqNs = drainRequestNanos.getAndSet(wid, -1L);
                            if (reqNs > 0) recordScaleInLatency(System.nanoTime() - reqNs);
                        }
                        continue;
                    }
                    fromLocal = true;
                } else { // INACTIVE (or transient ACTIVATING resolved above)
                    task = sharedQueue.poll(1, TimeUnit.MILLISECONDS);
                    fromLocal = false;
                }
            } catch (InterruptedException ie) {
                // Do not abort the drain: the while condition owns
                // termination. Clear interrupt (returning from poll
                // already cleared) and retry — shutdown + empty queues
                // will make the while condition exit naturally.
                if (shutdown.get() && sharedQueue.isEmpty() && local.isEmpty()) break;
                continue;
            }

            if (task == null) {
                // Shutdown drain assistance: once shutdown has been
                // requested, the scheduling-domain isolation between
                // ACTIVE (local only) and INACTIVE (shared only) is
                // no longer required. If a non-INACTIVE worker has an
                // empty local queue but sharedQueue still has LONG
                // work, it MUST help drain sharedQueue — otherwise
                // an all-ACTIVE configuration with pending LONG tasks
                // would loop forever without terminating.
                //
                // The steady-state loop (shutdown == false) is
                // unaffected: this branch is guarded by shutdown.get()
                // and only runs when the normal poll returned null.
                if (shutdown.get()) {
                    task = sharedQueue.poll();
                    if (task == null) continue;
                    fromLocal = false;
                } else {
                    continue;
                }
            }

            // Every task dequeued from a local queue is by construction
            // a SHORT (submit() only routes SHORT tasks to local queues).
            // Decrementing here — before task.run() — keeps Qs tightly
            // in sync with pending queued work and is the mirror of the
            // increment in submit().
            if (fromLocal) {
                queuedShortAdder.decrement();
            }

            long execStart = System.nanoTime();
            boolean taskThrew = false;
            try {
                task.run();
            } catch (RuntimeException taskEx) {
                // Task-scoped failure: do NOT let a broken workload
                // terminate this worker thread.
                //   - The task has already been dequeued from the local
                //     queue (queuedShortAdder was decremented above),
                //     so we must NOT restore the counter.
                //   - Worker state, activeShardCount and
                //     reservedShardedCapacity are untouched.
                //   - Failed tasks are excluded from the SHORT EWMA
                //     (the addShortSample call below is gated on
                //     !taskThrew), so a slow-fail cannot skew Ts.
                //   - We only catch RuntimeException around task.run()
                //     itself. Scheduler/internal exceptions elsewhere
                //     in the loop remain fail-fast by design.
                taskFailureCount.incrementAndGet();
                taskThrew = true;
                System.err.println("[DynamicHybrid] task "
                        + task.taskId() + " threw " + taskEx
                        + " — worker " + wid + " continuing");
            }
            long execNanos = System.nanoTime() - execStart;

            // P1 (fix 3): SHORT completion → feed the per-worker
            // packed sample slot with explicit saturation so a sum
            // overflow cannot corrupt the count field. Failed tasks
            // are excluded so Ts is not polluted by fault durations.
            if (fromLocal && !taskThrew) {
                addShortSample(wid, execNanos);
            }
        }
    }

    // =================================================================
    //  EWMA + pressure + controller policy
    // =================================================================

    /**
     * Test-only direct EWMA setter (was worker-side CAS pre-P1). In
     * production the controller applies the EWMA via
     * {@link #applyEwmaFromSamples()} once per tick from the striped
     * per-worker samples. Kept as a package-private hook so existing
     * unit tests remain deterministic.
     */
    void updateShortEwma(long sampleNanos) {
        double a = config.ewmaAlpha();
        long prev = shortEwmaNanos.get();
        long next;
        if (prev < 0) {
            next = sampleNanos;
        } else {
            double v = a * sampleNanos + (1.0 - a) * prev;
            next = Math.max(0L, (long) v);
        }
        shortEwmaNanos.set(next);
    }

    /**
     * Worker-side helper: atomically add one SHORT-completion sample
     * to this worker's packed accumulator.
     *
     * <p>Fix-2 semantics: a sample is accepted <b>only</b> if BOTH
     * {@code count + 1 <= SHORT_SAMPLE_COUNT_MAX} AND
     * {@code sum + execNanos <= SHORT_SAMPLE_SUM_MASK}. If either
     * bound would be exceeded, the packed slot is left unchanged and
     * the sample is dropped. This keeps the invariant "count and sum
     * always describe the same accepted sample set" — one field never
     * saturates independently of the other.
     *
     * <p>CAS loop, single writer (this worker), rare reader (controller
     * once per tick): the CAS almost always succeeds first try.
     */
    private void addShortSample(int wid, long execNanos) {
        // A sample larger than the sum field can ever hold is
        // unrepresentable — drop before touching the slot.
        if (execNanos < 0)                        return;
        if (execNanos > SHORT_SAMPLE_SUM_MASK)    return;
        while (true) {
            long prev      = perWorkerShortSamples.get(wid);
            long prevCount = prev >>> SHORT_SAMPLE_COUNT_SHIFT;
            long prevSum   = prev &   SHORT_SAMPLE_SUM_MASK;

            // Reject the sample entirely if EITHER field would overflow.
            // No independent field saturation — pair stays consistent.
            if (prevCount >= SHORT_SAMPLE_COUNT_MAX) return;
            if (prevSum   >  SHORT_SAMPLE_SUM_MASK - execNanos) return;

            long newCount = prevCount + 1L;
            long newSum   = prevSum + execNanos;
            long next     = (newCount << SHORT_SAMPLE_COUNT_SHIFT) | newSum;

            if (perWorkerShortSamples.compareAndSet(wid, prev, next)) return;
            // CAS lost — controller reset the slot to 0. Retry: the
            // sample is now welcome in the empty slot.
        }
    }

    /**
     * Controller-side EWMA update: called at most once per tick from
     * {@link #controllerTick()}. Reads and resets each worker's packed
     * (count, sum) sample slot with a single {@code getAndSet(0)}, so
     * count and sum for any given worker always come from the same
     * batch of tick-samples (fix 3). Aggregates all workers, computes
     * the tick's mean SHORT execution time (in nanoseconds), and folds
     * it into {@link #shortEwmaNanos} using the configured alpha.
     */
    private void applyEwmaFromSamples() {
        long totalCount    = 0L;
        long totalSumNanos = 0L;
        for (int i = 0; i < workerCount; i++) {
            long packed = perWorkerShortSamples.getAndSet(i, 0L);
            if (packed == 0L) continue;
            long count  = packed >>> SHORT_SAMPLE_COUNT_SHIFT;
            long sumNs  = packed &   SHORT_SAMPLE_SUM_MASK;
            totalCount    += count;
            totalSumNanos += sumNs;
        }
        if (totalCount <= 0) return;
        long tickMeanNanos = totalSumNanos / totalCount;
        double alpha = config.ewmaAlpha();
        long prev = shortEwmaNanos.get();
        long next;
        if (prev < 0) {
            next = tickMeanNanos;
        } else {
            double v = alpha * tickMeanNanos + (1.0 - alpha) * prev;
            next = Math.max(0L, (long) v);
        }
        shortEwmaNanos.set(next);   // controller is the only writer → plain set
    }

    /** Package-private accessor for tests. */
    long shortEwmaNanos() { return shortEwmaNanos.get(); }

    /**
     * Aggregate pending SHORT tasks — O(1) via {@link LongAdder#sum()}.
     * Used on the controller hot path in place of the previous O(N)
     * scan of every shard queue + every {@link #states} slot.
     *
     * <p>May return a small transient negative value under heavy
     * concurrent activity between {@code submit()} and worker dequeue
     * (LongAdder sums are point-in-time, not linearised across writes).
     * We clamp to zero before the caller uses it in the pressure
     * formula — this is a stable, well-known idiom for LongAdder
     * counters.
     */
    long queuedShortTasks() {
        long v = queuedShortAdder.sum();
        return v < 0 ? 0 : v;
    }

    /**
     * Exact O(N) scan across all shard queues, counting BOTH ACTIVE
     * and DRAINING (P2.8). This matches the {@link #queuedShortAdder}
     * semantics — tasks that were routed while ACTIVE remain "queued
     * short work" even after the shard transitions to DRAINING.
     * Kept for parity with the pre-optimization implementation and
     * used by unit tests to cross-check the LongAdder-based counter.
     * NOT used on the controller hot path.
     */
    long queuedShortTasksExact() {
        long total = 0L;
        for (int i = 0; i < workerCount; i++) {
            ShardState st = states.get(i);
            if (st == ShardState.ACTIVE || st == ShardState.DRAINING) {
                total += localQueues[i].size();
            }
        }
        return total;
    }

    /**
     * Compute Sharded pressure Ps = (Qs * Ts) / Ns, in nanoseconds.
     * Kept for backward compatibility and existing tests. Production
     * decisions go through {@link #snapshot()} which reads the state
     * once (P2.6).
     */
    long pressureNanos() {
        int ns = activeShardCount.get();
        long ts = shortEwmaNanos.get();
        long qs = queuedShortTasks();
        return calculatePressure(qs, ts, ns);
    }

    /**
     * Saturating pressure math (P2.7). Package-private for testing.
     * <ul>
     *   <li>{@code ns <= 0} → {@link Long#MAX_VALUE} (max pressure)</li>
     *   <li>{@code ts < 0} (uninitialised EWMA) → {@code 0}</li>
     *   <li>{@code qs * ts} overflow → {@link Long#MAX_VALUE}</li>
     * </ul>
     */
    static long calculatePressure(long qs, long ts, int ns) {
        if (ns <= 0) return Long.MAX_VALUE;
        if (ts < 0)  return 0L;
        if (qs <= 0 || ts == 0) return 0L;
        // Overflow guard: qs * ts must fit in signed long.
        if (qs > Long.MAX_VALUE / ts) return Long.MAX_VALUE;
        return (qs * ts) / ns;
    }

    /** Decision returned by {@link #evaluateScalingDecision()}. */
    enum Decision { NONE, SCALE_OUT, SCALE_IN }

    /**
     * Pure policy: given the current snapshot, decide at most one
     * capacity change per tick with hysteresis. Delegates to the
     * injected {@link CapacityPolicy} (default:
     * {@link HysteresisPressurePolicy}). Exposed package-private for
     * direct unit testing.
     */
    Decision evaluateScalingDecision() {
        return capacityPolicy.evaluate(snapshot());
    }

    /**
     * Package-private: build a lightweight point-in-time policy
     * snapshot. Each input is read exactly once. Individual fields are
     * atomic/visible, but the snapshot as a whole is not a globally
     * linearizable view across all scheduler state — a transition on
     * another thread may occur between two field reads within this
     * method. That is acceptable for the periodic control policy: the
     * next tick will see the newer values and correct if needed.
     */
    SchedulerSnapshot snapshot() {
        int  ns     = activeShardCount.get();
        long ts     = shortEwmaNanos.get();
        long qs     = queuedShortTasks();
        boolean shr = !sharedQueue.isEmpty();
        long ps     = calculatePressure(qs, ts, ns);
        return new SchedulerSnapshot(
                ps, ns, workerCount, config.minShardedWorkers(), maxShardedWorkers, shr,
                config.scaleOutThresholdMicros() * 1_000L,
                config.scaleInThresholdMicros()  * 1_000L);
    }

    /**
     * Request one INACTIVE shard to move to ACTIVATING. Returns the
     * shard id or -1 if none was available. Package-private for tests.
     *
     * <h4>Ordering (fixes 1 + 2)</h4>
     * <ol>
     *   <li><b>Reserve capacity first.</b> CAS-bumps
     *       {@link #reservedShardedCapacity} from {@code r} to
     *       {@code r+1} only if {@code r < maxShardedWorkers}. If we
     *       cannot reserve, we do NOT touch any per-shard state.</li>
     *   <li><b>Pre-filter shard candidates.</b> Only INACTIVE shards
     *       are candidates; we never touch a shard that is already
     *       ACTIVATING/ACTIVE/DRAINING.</li>
     *   <li><b>Publish timestamp via CAS(-1L, now).</b> This can only
     *       succeed if {@code activationRequestNanos[i]} is the "no
     *       pending" sentinel {@code -1}. If some other ACTIVATING
     *       request has already stamped it (transient window while a
     *       worker has not yet consumed the timestamp on
     *       {@code ACTIVATING -> ACTIVE}), we leave it untouched and
     *       try the next shard.</li>
     *   <li><b>Publish state via CAS(INACTIVE, ACTIVATING).</b> On
     *       success we own the transition and return the shard id.</li>
     *   <li><b>On any failure past step 1, release the reservation</b>
     *       and (if we set it) clear our timestamp via CAS(now, -1L).</li>
     * </ol>
     *
     * <p>Because timestamp writes are always CAS-guarded, an existing
     * ACTIVATING shard's request time is never overwritten. The cap
     * enforces
     * {@code reservedShardedCapacity <= maxShardedWorkers}, keeping at
     * least {@code minSharedWorkers} threads available for LONG work.
     */
    int requestScaleOut() {
        // Step 1: try to reserve one unit of sharded capacity. Never
        // publish ACTIVATING if this would exceed maxShardedWorkers.
        int r;
        do {
            r = reservedShardedCapacity.get();
            if (r >= maxShardedWorkers) return -1;
        } while (!reservedShardedCapacity.compareAndSet(r, r + 1));

        // Step 2..4: find an INACTIVE candidate and publish request.
        for (int i = 0; i < workerCount; i++) {
            if (states.get(i) != ShardState.INACTIVE) continue;

            // Timestamp CAS: only publish if slot is the -1 sentinel.
            long now = System.nanoTime();
            if (!activationRequestNanos.compareAndSet(i, -1L, now)) {
                // Already stamped by an ACTIVATING request that hasn't
                // yet been consumed by its worker — do NOT overwrite.
                continue;
            }

            if (states.compareAndSet(i, ShardState.INACTIVE, ShardState.ACTIVATING)) {
                scaleOutCount.incrementAndGet();
                return i;
            }

            // State CAS lost — rollback our timestamp only. Use CAS so
            // we cannot clobber a timestamp that some OTHER path has
            // meanwhile written.
            activationRequestNanos.compareAndSet(i, now, -1L);
            // Try next candidate; reservation stays held for retry.
        }

        // Step 5: no candidate accepted our reservation — release it.
        reservedShardedCapacity.decrementAndGet();
        return -1;
    }

    /**
     * Request one ACTIVE shard to move to DRAINING. Uses the per-shard
     * admission barrier: closes admissions FIRST, then transitions
     * state. This is the race barrier that guarantees no SHORT task
     * offered after the transition can be stranded (P0.1).
     * <p>Also refuses when {@code activeShardCount <= minShardedWorkers}
     * (spec floor). Decrements Ns immediately so the shard is
     * excluded from short-task routing on the next {@link #pickActiveShard()}.
     * Returns the shard id or -1 if none was available.
     */
    int requestScaleIn() {
        if (activeShardCount.get() <= config.minShardedWorkers()) return -1;
        for (int i = 0; i < workerCount; i++) {
            if (states.get(i) != ShardState.ACTIVE) continue;
            // 1) Close admissions FIRST — the strand-race barrier.
            //    If we cannot CAS 0 → MIN_VALUE, a submitter is currently
            //    mid-offer on this shard; skip and try the next ACTIVE.
            if (!admissionCount.compareAndSet(i, 0, Integer.MIN_VALUE)) continue;
            // 2) Publish drain-request timestamp BEFORE the state
            //    transition becomes visible (P1 fix 4). Use CAS from
            //    the -1 sentinel so we cannot clobber an existing
            //    drain-request that a test hook / stale run left behind.
            long now = System.nanoTime();
            if (!drainRequestNanos.compareAndSet(i, -1L, now)) {
                // Existing pending drain — reopen admissions and skip.
                admissionCount.set(i, 0);
                continue;
            }
            // 3) State transition. Under the single-controller invariant
            //    no one else can flip ACTIVE → non-ACTIVE except us.
            if (!states.compareAndSet(i, ShardState.ACTIVE, ShardState.DRAINING)) {
                // Extremely rare (e.g. forceState from a test). Rollback
                // timestamp and admission barrier atomically-observably.
                drainRequestNanos.compareAndSet(i, now, -1L);
                admissionCount.set(i, 0);
                continue;
            }
            // 4) Only activeShardCount (Ns) drops here — the reservation
            //    stays held. A DRAINING worker is still consuming its
            //    local SHORT queue and therefore is NOT available to
            //    service sharedQueue/LONG work. reservedShardedCapacity
            //    is released only when the worker publishes
            //    DRAINING -> INACTIVE.
            activeShardCount.decrementAndGet();
            scaleInCount.incrementAndGet();
            rebuildActiveSnapshot();   // ACTIVE set changed
            return i;
        }
        return -1;
    }

    private void recordScaleOutLatency(long elapsedNs) {
        if (elapsedNs < 0) return;
        // Publish max/total BEFORE count so any observer that sees
        // count > 0 sees a consistent (max, total) pair.
        scaleOutLatencyMaxNs.accumulateAndGet(elapsedNs, Math::max);
        scaleOutLatencyTotalNs.addAndGet(elapsedNs);
        scaleOutLatencyCount.incrementAndGet();
    }

    private void recordScaleInLatency(long elapsedNs) {
        if (elapsedNs < 0) return;
        scaleInLatencyMaxNs.accumulateAndGet(elapsedNs, Math::max);
        scaleInLatencyTotalNs.addAndGet(elapsedNs);
        scaleInLatencyCount.incrementAndGet();
    }

    private void controllerTick() {
        // Primitive-only instrumentation: two nanoTime() calls + a
        // handful of atomic adds per tick. No object allocation.
        final long start = System.nanoTime();

        // P1: actual controller interval (scheduling jitter).
        long prevStart = previousTickStartNanos;
        previousTickStartNanos = start;
        if (prevStart != 0L) {
            long actual = start - prevStart;
            if (actual > 0) {
                controllerIntervalSamples.incrementAndGet();
                controllerIntervalTotalNs.addAndGet(actual);
                controllerIntervalMaxNs.accumulateAndGet(actual, Math::max);
            }
        }

        try {
            // P1: fold accumulated per-worker SHORT samples into the
            //     executor-level EWMA. O(#stripes), still fast.
            applyEwmaFromSamples();

            Decision d = evaluateScalingDecision();
            switch (d) {
                case SCALE_OUT -> requestScaleOut();
                case SCALE_IN  -> requestScaleIn();
                case NONE      -> { /* no-op */ }
            }
        } catch (Throwable t) {
            // Never let the controller crash the executor.
            System.err.println("[DynamicHybrid] controller tick failed: " + t);
        } finally {
            long elapsed = System.nanoTime() - start;
            if (elapsed < 0) elapsed = 0;   // nanoTime paranoia
            controllerTickCount.incrementAndGet();
            controllerTotalRuntimeNanos.addAndGet(elapsed);
            controllerMaxRuntimeNanos.accumulateAndGet(elapsed, Math::max);
            if (elapsed > controllerPeriodNanos) {
                controllerOverrunCount.incrementAndGet();
            }
        }
    }

    // =================================================================
    //  Test / metrics accessors
    // =================================================================

    int workerCount()          { return workerCount; }
    int activeShardCount()     { return activeShardCount.get(); }
    /**
     * Package-private accessor for tests: number of workers reserved
     * to the sharded domain — i.e. those in
     * {@code ACTIVE + ACTIVATING + DRAINING}.
     */
    int reservedShardedCapacity() { return reservedShardedCapacity.get(); }
    /** Package-private accessor for tests: raw packed sample slot. */
    long perWorkerShortSampleSlot(int wid) { return perWorkerShortSamples.get(wid); }
    /** Package-private test hook: directly set a slot (bypasses saturation). */
    void perWorkerShortSampleSetForTests(int wid, long value) { perWorkerShortSamples.set(wid, value); }
    /** Package-private test hook: feed a SHORT sample as if the worker had. */
    void addShortSampleForTests(int wid, long execNanos) { addShortSample(wid, execNanos); }
    ShardState state(int wid)  { return states.get(wid); }
    void forceState(int wid, ShardState s) {
        // Test-only helper. Keeps activeShardCount and
        // reservedShardedCapacity consistent when toggling
        // ACTIVE ↔ non-ACTIVE, and rebuilds the RR snapshot so
        // pickActiveShard() sees the forced state.
        //
        // Note: reservedShardedCapacity == ACTIVE + ACTIVATING + DRAINING
        // under the normal transition sequence — i.e. all workers not
        // currently servicing sharedQueue. Tests that forceState directly
        // into ACTIVATING (without going through requestScaleOut) can
        // temporarily desynchronize the counter — those tests should
        // not assert on reservedShardedCapacity, and the constraint
        // "reservedShardedCapacity <= maxShardedWorkers" is enforced
        // by requestScaleOut only.
        ShardState prev = states.getAndSet(wid, s);
        boolean activeChanged = false;
        if (prev != ShardState.ACTIVE && s == ShardState.ACTIVE) {
            activeShardCount.incrementAndGet();
            activeChanged = true;
        } else if (prev == ShardState.ACTIVE && s != ShardState.ACTIVE) {
            activeShardCount.decrementAndGet();
            activeChanged = true;
        }
        // Reservation tracks all states that are NOT servicing sharedQueue.
        boolean wasReserving = isReservingState(prev);
        boolean nowReserving = isReservingState(s);
        if (!wasReserving && nowReserving) {
            reservedShardedCapacity.incrementAndGet();
        } else if (wasReserving && !nowReserving) {
            reservedShardedCapacity.decrementAndGet();
        }
        if (activeChanged) rebuildActiveSnapshot();
    }

    /** Which shard states count toward {@link #reservedShardedCapacity}. */
    private static boolean isReservingState(ShardState s) {
        return s == ShardState.ACTIVE
                || s == ShardState.ACTIVATING
                || s == ShardState.DRAINING;
    }

    /**
     * Test-only helper: mimics the admission-barrier close that
     * {@link #requestScaleIn()} performs, without going through the
     * Nmin floor check. Used by tests that reach DRAINING via
     * {@link #forceState}.
     */
    void closeAdmissionsForTests(int wid) {
        admissionCount.set(wid, Integer.MIN_VALUE);
    }

    /**
     * Test-only helper: mirrors the {@code admissionCount[wid] = 0}
     * that the worker performs at {@code DRAINING -> INACTIVE}. Used
     * by the stale-snapshot admission-race test to reproduce the
     * exact reopened-admissions state.
     */
    void reopenAdmissionsForTests(int wid) {
        admissionCount.set(wid, 0);
    }

    /** Test-only: read the raw admissionCount slot. */
    int admissionCountForTests(int wid) {
        return admissionCount.get(wid);
    }

    /** Test-only: read the raw activation-request timestamp slot. */
    long activationRequestNanosForTests(int wid) {
        return activationRequestNanos.get(wid);
    }

    /** Test-only: write the raw activation-request timestamp slot. */
    void activationRequestNanosSetForTests(int wid, long value) {
        activationRequestNanos.set(wid, value);
    }

    /**
     * Test-only: force a shard state WITHOUT rebuilding the RR
     * snapshot. Deliberately used to construct a stale snapshot in the
     * "stale snapshot rejected by admission barrier" test.
     */
    void forceStateWithoutRebuildForTests(int wid, ShardState s) {
        ShardState prev = states.getAndSet(wid, s);
        if (prev != ShardState.ACTIVE && s == ShardState.ACTIVE) {
            activeShardCount.incrementAndGet();
        } else if (prev == ShardState.ACTIVE && s != ShardState.ACTIVE) {
            activeShardCount.decrementAndGet();
        }
        boolean wasReserving = isReservingState(prev);
        boolean nowReserving = isReservingState(s);
        if (!wasReserving && nowReserving) {
            reservedShardedCapacity.incrementAndGet();
        } else if (wasReserving && !nowReserving) {
            reservedShardedCapacity.decrementAndGet();
        }
    }
    LinkedBlockingQueue<Task> sharedQueue() { return sharedQueue; }
    LinkedBlockingQueue<Task> localQueue(int wid) { return localQueues[wid]; }
    long scaleOutCount()       { return scaleOutCount.get(); }
    long scaleInCount()        { return scaleInCount.get(); }
    long shortFallbackCount()  { return shortFallbackCount.get(); }
    /** Package-private accessor for tests: RuntimeExceptions thrown by workloads. */
    long taskFailureCount()    { return taskFailureCount.get(); }
    DynamicHybridConfig config() { return config; }

    /* ---- Controller runtime metrics (public: read by BenchmarkMain) ---- */
    public long controllerTickCount()         { return controllerTickCount.get(); }
    public long controllerTotalRuntimeNanos() { return controllerTotalRuntimeNanos.get(); }
    public long controllerMaxRuntimeNanos()   { return controllerMaxRuntimeNanos.get(); }
    public long controllerOverrunCount()      { return controllerOverrunCount.get(); }
    public long controllerPeriodNanos()       { return controllerPeriodNanos; }
    /** Mean runtime per tick, in ns. Returns 0 when no tick has run yet. */
    public long controllerMeanRuntimeNanos() {
        long n = controllerTickCount.get();
        return n == 0 ? 0L : controllerTotalRuntimeNanos.get() / n;
    }

    /* ---- P1: Transition-latency metrics ---- */
    public long scaleOutLatencyCount()      { return scaleOutLatencyCount.get(); }
    public long scaleOutLatencyTotalNanos() { return scaleOutLatencyTotalNs.get(); }
    public long scaleOutLatencyMaxNanos()   { return scaleOutLatencyMaxNs.get(); }
    public long scaleOutLatencyMeanNanos() {
        long c = scaleOutLatencyCount.get();
        return c == 0 ? 0L : scaleOutLatencyTotalNs.get() / c;
    }
    public long scaleInLatencyCount()       { return scaleInLatencyCount.get(); }
    public long scaleInLatencyTotalNanos()  { return scaleInLatencyTotalNs.get(); }
    public long scaleInLatencyMaxNanos()    { return scaleInLatencyMaxNs.get(); }
    public long scaleInLatencyMeanNanos() {
        long c = scaleInLatencyCount.get();
        return c == 0 ? 0L : scaleInLatencyTotalNs.get() / c;
    }

    /* ---- P1: Actual controller-interval jitter metrics ---- */
    public long controllerIntervalSamples()      { return controllerIntervalSamples.get(); }
    public long controllerIntervalTotalNanos()   { return controllerIntervalTotalNs.get(); }
    public long controllerIntervalMaxNanos()     { return controllerIntervalMaxNs.get(); }
    public long controllerIntervalMeanNanos() {
        long c = controllerIntervalSamples.get();
        return c == 0 ? 0L : controllerIntervalTotalNs.get() / c;
    }

    /** Cap on ACTIVE shards. */
    public int maxShardedWorkers() { return maxShardedWorkers; }

    /**
     * Snapshot of the state distribution across all N workers.
     * Order: [INACTIVE, ACTIVATING, ACTIVE, DRAINING].
     */
    int[] stateHistogram() {
        int[] h = new int[4];
        for (int i = 0; i < workerCount; i++) {
            h[states.get(i).ordinal()]++;
        }
        return h;
    }

    // =================================================================
    //  Lifecycle
    // =================================================================

    @Override
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) return;
        controller.shutdownNow();
        // Workers poll with a 1 ms timeout — they observe the shutdown
        // flag within one poll cycle and exit their loop naturally after
        // draining their local queue AND helping drain the shared queue
        // (the while-condition already covers this). We deliberately do
        // NOT interrupt workers here: interruption races with poll() and
        // could lose an already-dequeued task (P2.9).
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (Thread t : workerThreads) {
            long rem = deadline - System.nanoTime();
            if (rem <= 0) break;
            // Split remaining nanos into (millis, nanos) so a sub-ms
            // budget doesn't degenerate into t.join(0) — which means
            // "wait forever" in Thread.join(long).
            long millis = TimeUnit.NANOSECONDS.toMillis(rem);
            int nanos = (int) (rem - TimeUnit.MILLISECONDS.toNanos(millis));
            if (nanos < 0) nanos = 0;
            if (nanos > 999_999) nanos = 999_999;
            if (millis == 0 && nanos == 0) nanos = 1;   // never call join(0,0)
            t.join(millis, nanos);
        }
        // Deadline expired: forcefully interrupt lingering workers so
        // shutdown does not hang indefinitely. Anything still queued at
        // this point is dropped — the caller's timeout was our budget
        // for graceful drain.
        boolean allDone = true;
        for (Thread t : workerThreads) {
            if (t.isAlive()) { t.interrupt(); allDone = false; }
        }
        return allDone;
    }

    @Override
    public String label() { return "DynamicHybrid"; }

    @Override
    public int totalQueueSize() {
        int total = sharedQueue.size();
        for (LinkedBlockingQueue<Task> q : localQueues) total += q.size();
        return total;
    }

    /** Human-readable summary line for the run summary file. */
    public String metricsSummary() {
        int[] h = stateHistogram();
        return "activeShardedWorkers=" + h[ShardState.ACTIVE.ordinal()]
                + ", activatingShards=" + h[ShardState.ACTIVATING.ordinal()]
                + ", drainingShards=" + h[ShardState.DRAINING.ordinal()]
                + ", inactiveShards=" + h[ShardState.INACTIVE.ordinal()]
                + ", shortTaskEwmaMicros=" + (shortEwmaNanos.get() < 0 ? "n/a" : (shortEwmaNanos.get() / 1000L))
                + ", scaleOutCount=" + scaleOutCount.get()
                + ", scaleInCount=" + scaleInCount.get()
                + ", shortFallbackCount=" + shortFallbackCount.get()
                + ", taskFailureCount=" + taskFailureCount.get()
                + ", scaleOutLatencyMeanNs=" + scaleOutLatencyMeanNanos()
                + ", scaleOutLatencyMaxNs=" + scaleOutLatencyMaxNs.get()
                + ", scaleInLatencyMeanNs=" + scaleInLatencyMeanNanos()
                + ", scaleInLatencyMaxNs=" + scaleInLatencyMaxNs.get()
                + ", controllerTicks=" + controllerTickCount.get()
                + ", controllerMeanNs=" + controllerMeanRuntimeNanos()
                + ", controllerMaxNs=" + controllerMaxRuntimeNanos.get()
                + ", controllerOverruns=" + controllerOverrunCount.get()
                + ", controllerPeriodNs=" + controllerPeriodNanos
                + ", controllerIntervalMeanNs=" + controllerIntervalMeanNanos()
                + ", controllerIntervalMaxNs=" + controllerIntervalMaxNs.get()
                + ", maxShardedWorkers=" + maxShardedWorkers
                + ", reservedShardedCapacity=" + reservedShardedCapacity.get();
    }

    /** Best-effort dump of active shard ids. */
    @SuppressWarnings("unused")
    public List<Integer> activeShardIds() {
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            if (states.get(i) == ShardState.ACTIVE) out.add(i);
        }
        return out;
    }
}

