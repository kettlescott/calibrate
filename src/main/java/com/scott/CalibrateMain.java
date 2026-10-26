package com.scott;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class CalibrateMain {

    private CalibrateMain() {
    }

    public static void main(String[] args) throws Exception {
        Path configPath = parseConfigPath(args);
        CalibrateConfig config = CalibrateConfigLoader.load(configPath);
        new CalibrateRunner().run(config);
    }

    private static Path parseConfigPath(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--config=")) {
                return Paths.get(arg.substring("--config=".length()).trim());
            }
        }
        return Paths.get("caliberation.yaml");
    }
}

