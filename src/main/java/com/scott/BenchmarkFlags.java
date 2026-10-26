package com.scott;

/**
 * Centralized benchmark configuration flags.
 *
 * <p>{@code DEBUG} controls whether extra consistency checks, bounds
 * verification, task-list tracking, and diagnostic logging are enabled.
 * When {@code false} (the default), all debug code paths are
 * dead-code-eliminated by the JIT, producing <em>zero</em> overhead on
 * the hot path.
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Normal benchmark (minimal overhead):
 *   java -cp ... com.scott.BenchmarkMain
 *
 *   # Debug mode (extra checks + logging + task tracking):
 *   java -Dbenchmark.debug=true -cp ... com.scott.BenchmarkMain
 * </pre>
 *
 * <p>Because this field is {@code static final}, the JIT constant-folds
 * it at class-load time.  Any {@code if (BenchmarkFlags.DEBUG)} block
 * whose body has no side-effects visible outside that block is eliminated
 * entirely — no branch, no dead code, no residual overhead.
 */
final class BenchmarkFlags {

    /**
     * When {@code true}, enables:
     * <ul>
     *   <li>Per-submit task-list tracking in {@link SharedExecutor} and
     *       {@link ShardedExecutor} (for {@code getTasks()} diagnostics)</li>
     * </ul>
     * When {@code false} (default), all of the above are skipped.
     */
    static final boolean DEBUG = Boolean.getBoolean("benchmark.debug");

    private BenchmarkFlags() { }
}

