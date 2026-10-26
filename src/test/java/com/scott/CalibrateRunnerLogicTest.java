package com.scott;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CalibrateRunnerLogicTest {

    @Test
    void stopConditionMatchesSpec() {
        assertTrue(CalibrateRunner.shouldStop(10.0, 9.0));
        assertFalse(CalibrateRunner.shouldStop(9.0, 10.0));
        assertFalse(CalibrateRunner.shouldStop(10.0, 10.0));
    }

    @Test
    void generatedDurationsHaveExpectedShape() {
        List<Long> durations = WorkloadGenerator.generateMicros("uniform", 50, 0.2, 200_000, 123L);
        assertTrue(durations.size() == 200_000);
        assertTrue(durations.stream().allMatch(v -> v > 0));
    }
}

