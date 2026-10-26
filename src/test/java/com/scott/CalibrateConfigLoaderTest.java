package com.scott;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CalibrateConfigLoaderTest {

    @Test
    void loadsCalibrateYaml() throws Exception {
        String yaml = """
                global:
                  workerCount: 32
                  maxInflight: 32
                  seed: 3735928559
                  warmupSeconds: 0
                  measurementSeconds: 20
                  binarySearchRepeats: 3
                  BINARY_CV_EPSILON: 1.0e-6

                calibrate:
                  cpu_50us:
                    - kind: CPU
                      distribution: uniform
                      start_cv: 0.01
                      max: 0.57
                      duration: 50
                """;

        Path tmp = Files.createTempFile("calibrate", ".yaml");
        Files.writeString(tmp, yaml);

        CalibrateConfig config = CalibrateConfigLoader.load(tmp);
        assertEquals(32, config.global().workerCount());
        assertEquals(3, config.binarySearchRepeats());
        assertEquals(1.0e-6, config.binaryCvEps());
        assertEquals(1, config.calibrate().size());
        CalibrateEntry entry = config.calibrate().get("cpu_50us").get(0);
        assertEquals(WorkloadKind.CPU, entry.kind());
        assertEquals("uniform", entry.distribution());
        assertEquals(0.01, entry.startCv());
        assertEquals(0.57, entry.maxCv());
        assertEquals(50L, entry.durationMicros());
    }

    @Test
    void defaultsBinarySearchRepeatsToOne() throws Exception {
        String yaml = """
                global:
                  workerCount: 32
                  maxInflight: 32
                  seed: 3735928559
                  warmupSeconds: 0
                  measurementSeconds: 20

                calibrate:
                  cpu_50us:
                    - kind: CPU
                      distribution: uniform
                      start_cv: 0.01
                      max: 0.57
                      duration: 50
                """;

        Path tmp = Files.createTempFile("calibrate", ".yaml");
        Files.writeString(tmp, yaml);

        CalibrateConfig config = CalibrateConfigLoader.load(tmp);
        assertEquals(1, config.binarySearchRepeats());
        assertEquals(1e-7, config.binaryCvEps());
    }
}

