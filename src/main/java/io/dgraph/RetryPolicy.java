/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Configures retry behavior for {@link DgraphClient#withRetry} and {@link
 * DgraphAsyncClient#withRetry}. Immutable — create instances via {@link #builder()}.
 *
 * <p>Backoff formula: {@code min(baseDelay * 2^attempt, maxDelay) + random(0, delay * jitter)}.
 *
 * <p>Common configurations:
 *
 * <pre>{@code
 * RetryPolicy.DEFAULT                          // read-write, 5 retries, 100ms base
 * RetryPolicy.readOnly()                       // read-only, default retry params
 * RetryPolicy.bestEffort()                     // read-only + best-effort
 * RetryPolicy.builder().maxRetries(10).build() // custom
 * }</pre>
 */
public final class RetryPolicy {

  /** Read-write transaction, 5 retries, 100ms base delay, 5s max delay, 10% jitter. */
  public static final RetryPolicy DEFAULT = builder().build();

  private final int maxRetries;
  private final Duration baseDelay;
  private final Duration maxDelay;
  private final double jitter;
  private final boolean readOnly;
  private final boolean bestEffort;

  private RetryPolicy(Builder builder) {
    this.maxRetries = builder.maxRetries;
    this.baseDelay = builder.baseDelay;
    this.maxDelay = builder.maxDelay;
    this.jitter = builder.jitter;
    this.readOnly = builder.readOnly;
    this.bestEffort = builder.bestEffort;
  }

  /** Returns a read-only policy with default retry parameters. */
  public static RetryPolicy readOnly() {
    return builder().readOnly().build();
  }

  /** Returns a read-only + best-effort policy with default retry parameters. */
  public static RetryPolicy bestEffort() {
    return builder().bestEffort().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public Duration getBaseDelay() {
    return baseDelay;
  }

  public Duration getMaxDelay() {
    return maxDelay;
  }

  public double getJitter() {
    return jitter;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public boolean isBestEffort() {
    return bestEffort;
  }

  /**
   * Calculates the delay in milliseconds for the given attempt number using exponential backoff
   * with jitter.
   */
  long calculateDelay(int attempt) {
    long base = baseDelay.toMillis();
    long max = maxDelay.toMillis();
    long delay = Math.min(base * (1L << attempt), max);
    if (jitter > 0) {
      delay += (long) (delay * jitter * ThreadLocalRandom.current().nextDouble());
    }
    return delay;
  }

  public static final class Builder {
    private int maxRetries = 5;
    private Duration baseDelay = Duration.ofMillis(100);
    private Duration maxDelay = Duration.ofSeconds(5);
    private double jitter = 0.1;
    private boolean readOnly = false;
    private boolean bestEffort = false;

    private Builder() {}

    public Builder maxRetries(int maxRetries) {
      if (maxRetries < 0) {
        throw new IllegalArgumentException("maxRetries must be >= 0");
      }
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder baseDelay(Duration baseDelay) {
      if (baseDelay.isNegative()) {
        throw new IllegalArgumentException("baseDelay must be >= 0");
      }
      this.baseDelay = baseDelay;
      return this;
    }

    public Builder maxDelay(Duration maxDelay) {
      if (maxDelay.isNegative()) {
        throw new IllegalArgumentException("maxDelay must be >= 0");
      }
      this.maxDelay = maxDelay;
      return this;
    }

    public Builder jitter(double jitter) {
      if (jitter < 0 || jitter > 1) {
        throw new IllegalArgumentException("jitter must be between 0 and 1");
      }
      this.jitter = jitter;
      return this;
    }

    public Builder readOnly() {
      this.readOnly = true;
      return this;
    }

    /** Enables best-effort mode. Implies read-only. */
    public Builder bestEffort() {
      this.bestEffort = true;
      this.readOnly = true;
      return this;
    }

    public RetryPolicy build() {
      return new RetryPolicy(this);
    }
  }
}
