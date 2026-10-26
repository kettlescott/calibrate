package com.scott;

/**
 * Controlled sharded-load-imbalance configuration for Experiment 2.
 *
 * <p>Given N shards and a skew parameter {@code alpha in [0,1]}:
 *
 * <pre>
 *   p_hot   = alpha + (1 - alpha) / N
 *   p_other = (1 - alpha) / N
 * </pre>
 *
 * <p>{@code alpha == 0.0} represents the balanced Experiment 2
 * baseline. {@code alpha == 1.0} routes all offered work to
 * {@link #hotShardId}. {@code alpha} is a skew parameter, not an
 * enable flag.
 *
 * <p>The presence of a {@code ShardImbalanceConfig} enables the
 * Experiment 2 workload path. Legacy behaviour is represented by a
 * {@code null} configuration, <em>not</em> by {@code alpha == 0}.
 *
 * <p>The workload generator selects routing keys whose normal
 * {@link Hashing#shardOf} result corresponds to the planned target
 * shard. The production routing pipeline therefore remains unchanged:
 *
 * <pre>
 *   Task
 *     -&gt; routingKey
 *     -&gt; Hashing.shardOf(...)
 *     -&gt; shard
 * </pre>
 *
 * <p>{@link #randomSeed} controls the deterministic Fisher-Yates
 * shuffle of the planned shard-assignment sequence.
 */
public record ShardImbalanceConfig(double alpha, int hotShardId, long randomSeed) {

    public ShardImbalanceConfig {
        if (Double.isNaN(alpha) || alpha < 0.0 || alpha > 1.0) {
            throw new IllegalArgumentException(
                    "shardImbalance.alpha must be in [0,1] (was " + alpha + ")");
        }
        if (hotShardId < 0) {
            throw new IllegalArgumentException(
                    "shardImbalance.hotShardId must be >= 0 (was " + hotShardId + ")");
        }
    }

    public void validate(int workerCount) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException(
                    "workerCount must be > 0 (was " + workerCount + ")");
        }
        if (hotShardId >= workerCount) {
            throw new IllegalArgumentException(
                    "shardImbalance.hotShardId=" + hotShardId
                            + " must be < workerCount=" + workerCount);
        }
    }

    public String describe() {
        return "alpha=" + alpha + ", hotShardId=" + hotShardId
                + ", randomSeed=" + randomSeed;
    }
}

