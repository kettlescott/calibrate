package com.scott;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkloadGenerator {

  /**
   * Reusable in-memory generator for benchmark durations.
   */
  public static List<Long> generateMicros(String distribution,
                                          long durationUs,
                                          double cv,
                                          int count,
                                          long seed) {
    Config config = new Config(durationUs, cv, distribution, count, seed, null);
    config.validate();

    List<Double> durations = new ArrayList<>(count);
    Random random = new Random(seed);
    switch (config.distribution) {
      case "two-point" -> generateTwoPoint(durations, config, random);
      case "uniform" -> generateUniform(durations, config, random);
      case "lognormal" -> generateLognormal(durations, config, random);
      default -> throw new IllegalArgumentException("Unknown distribution: " + config.distribution);
    }
    return convertToLong(durations);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java WorkloadGenerator <config.yaml>");
      System.exit(1);
    }

    String configFile = args[0];
    Config config = loadConfig(configFile);
    config.validate();

    List<Double> durations = generateDurations(config);
    writeOutput(config.output, durations);
    printStatistics(durations);
  }

  private static Config loadConfig(String filename) throws IOException {
    Yaml yaml = new Yaml();
    try (FileInputStream input = new FileInputStream(filename)) {
      Map<String, Object> data = yaml.load(input);
      @SuppressWarnings("unchecked")
      Map<String, Object> workload = (Map<String, Object>) data.get("workload");
      return new Config(workload);
    }
  }

  private static List<Double> generateDurations(Config config) {
    List<Double> durations = new ArrayList<>();
    Random random = new Random(config.seed);

    if ("two-point".equals(config.distribution)) {
      generateTwoPoint(durations, config, random);
    } else if ("uniform".equals(config.distribution)) {
      generateUniform(durations, config, random);
    } else if ("lognormal".equals(config.distribution)) {
      generateLognormal(durations, config, random);
    } else {
      throw new IllegalArgumentException("Unknown distribution: " + config.distribution);
    }

    return durations;
  }

  private static void generateTwoPoint(List<Double> durations, Config config, Random random) {
    double low = config.t_us * (1 - config.cv);
    double high = config.t_us * (1 + config.cv);

    int lowCount = config.count / 2;
    int highCount = config.count / 2;

    if (config.count % 2 == 1) {
      if (random.nextBoolean()) {
        lowCount++;
      } else {
        highCount++;
      }
    }

    for (int i = 0; i < lowCount; i++) {
      durations.add(low);
    }
    for (int i = 0; i < highCount; i++) {
      durations.add(high);
    }

    Collections.shuffle(durations, random);
  }

  private static List<Long> generateUniform(List<Double> durations, Config config, Random random) {
    double range = 2 * Math.sqrt(3) * config.cv * config.t_us;
    double min = config.t_us - range / 2;

    for (int i = 0; i < config.count; i++) {
      durations.add(min + random.nextDouble() * range);
    }
    return convertToLong(durations);
  }

  public static List<Long> generateLognormal(List<Double> durations, Config config, Random random) {
    double sigmaSquared = Math.log1p(config.cv * config.cv);
    double sigma = Math.sqrt(sigmaSquared);
    double mu = Math.log(config.t_us) - sigmaSquared / 2;

    for (int i = 0; i < config.count; i++) {
      durations.add(Math.exp(mu + sigma * random.nextGaussian()));
    }
    return convertToLong(durations);
  }

  private static List<Long> convertToLong(List<Double> durations){
    List<Long> ret = new ArrayList<>();
    for (double duration : durations) {
      ret.add(Math.round(duration));
    }
    return ret;
  }

  private static void writeOutput(String filename, List<Double> durations) throws IOException {
    try (FileWriter writer = new FileWriter(filename)) {
      for (double duration : durations) {
        // Round to nearest integer
        long rounded = Math.round(duration);
        writer.write(String.format("%d\n", rounded));
      }
    }
  }

  private static void printStatistics(List<Double> durations) {
    double mean = durations.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    double variance = durations.stream()
        .mapToDouble(d -> Math.pow(d - mean, 2))
        .average()
        .orElse(0);
    double stdDev = Math.sqrt(variance);
    double cv = stdDev / mean;

    double min = durations.stream().mapToDouble(Double::doubleValue).min().orElse(0);
    double max = durations.stream().mapToDouble(Double::doubleValue).max().orElse(0);

    System.out.printf("Mean: %.2f us\n", mean);
    System.out.printf("StdDev: %.2f us\n", stdDev);
    System.out.printf("CV: %.4f\n", cv);
    System.out.printf("Min: %.2f us\n", min);
    System.out.printf("Max: %.2f us\n", max);
  }

  private static class Config {
    double t_us;
    double cv;
    String distribution;
    int count;
    long seed;
    String output;

    Config(double t_us, double cv, String distribution, int count, long seed, String output) {
      this.t_us = t_us;
      this.cv = cv;
      this.distribution = distribution;
      this.count = count;
      this.seed = seed;
      this.output = output;
    }

    Config(Map<String, Object> data) {
      this.t_us = getDouble(data, "t_us");
      this.cv = getDouble(data, "cv");
      this.distribution = getString(data, "distribution");
      this.count = getInt(data, "count");
      this.seed = getLong(data, "seed");
      this.output = getString(data, "output");
    }

    void validate() {
      if (t_us <= 0) {
        throw new IllegalArgumentException("t_us must be > 0");
      }
      if (count <= 0) {
        throw new IllegalArgumentException("count must be > 0");
      }

      double sqrtThree = Math.sqrt(3);
      if ("two-point".equals(distribution)) {
        if (cv < 0 || cv > 1) {
          throw new IllegalArgumentException("two-point: CV must be in [0, 1]");
        }
      } else if ("uniform".equals(distribution)) {
        if (cv < 0 || cv > 1 / sqrtThree) {
          throw new IllegalArgumentException(
              String.format("uniform: CV must be in [0, %.4f]", 1 / sqrtThree));
        }
      } else if ("lognormal".equals(distribution)) {
        if (cv < 0) {
          throw new IllegalArgumentException("lognormal: CV must be >= 0");
        }
      } else {
        throw new IllegalArgumentException(
            "distribution must be 'two-point', 'uniform', or 'lognormal'");
      }
    }

    private double getDouble(Map<String, Object> data, String key) {
      Object value = data.get(key);
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      throw new IllegalArgumentException("Missing or invalid: " + key);
    }

    private int getInt(Map<String, Object> data, String key) {
      Object value = data.get(key);
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      throw new IllegalArgumentException("Missing or invalid: " + key);
    }

    private long getLong(Map<String, Object> data, String key) {
      Object value = data.get(key);
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      throw new IllegalArgumentException("Missing or invalid: " + key);
    }

    private String getString(Map<String, Object> data, String key) {
      Object value = data.get(key);
      if (value instanceof String) {
        return (String) value;
      }
      throw new IllegalArgumentException("Missing or invalid: " + key);
    }
  }
}

