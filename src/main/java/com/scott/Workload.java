package com.scott;

/**
 * Represents a synthetic benchmark workload.
 * <p>
 * Implementations perform a unit of work and return an observable result
 * so the JIT cannot eliminate the computation as dead code.
 */
public interface Workload {
    long execute();
}

