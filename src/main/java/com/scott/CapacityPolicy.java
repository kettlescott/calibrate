package com.scott;

import com.scott.DynamicHybridDispatcher.Decision;

/**
 * Pluggable capacity policy for {@link DynamicHybridDispatcher}.
 *
 * <p>Given a {@link SchedulerSnapshot} produced by the controller, a
 * policy returns at most one capacity change per invocation:
 * {@link Decision#SCALE_OUT}, {@link Decision#SCALE_IN}, or
 * {@link Decision#NONE}.
 *
 * <p>Deliberately minimal (spec: "do not add factories, registries,
 * plugin frameworks, dependency injection"). The interface is a plain
 * functional type so tests / experiments can pass a lambda, and
 * production code injects {@link HysteresisPressurePolicy} — the
 * default EWMA + Ps / H / L implementation.
 *
 * <p>Implementations MUST be pure with respect to their input snapshot
 * (no I/O, no blocking, no touching dispatcher internals) and MUST
 * respect the following invariants:
 * <ul>
 *   <li>never return {@code SCALE_OUT} when {@code ns >= N}</li>
 *   <li>never return {@code SCALE_IN}  when {@code ns <= nmin}</li>
 *   <li>never return {@code SCALE_IN}  when the shared queue is empty</li>
 * </ul>
 * These same guards are also enforced by
 * {@link DynamicHybridDispatcher#requestScaleOut()} /
 * {@link DynamicHybridDispatcher#requestScaleIn()} as a safety net.
 */
@FunctionalInterface
public interface CapacityPolicy {

    Decision evaluate(SchedulerSnapshot snapshot);
}

