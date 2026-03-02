/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import java.time.Duration;
import org.testng.annotations.Test;

/** Tests for {@link RetryPolicy} builder, defaults, and delay calculation. */
public class RetryPolicyTest {

  @Test
  public void testDefaults() {
    RetryPolicy policy = RetryPolicy.DEFAULT;
    assertEquals(policy.getMaxRetries(), 5);
    assertEquals(policy.getBaseDelay(), Duration.ofMillis(100));
    assertEquals(policy.getMaxDelay(), Duration.ofSeconds(5));
    assertEquals(policy.getJitter(), 0.1);
    assertFalse(policy.isReadOnly());
    assertFalse(policy.isBestEffort());
  }

  @Test
  public void testReadOnlyFactory() {
    RetryPolicy policy = RetryPolicy.readOnly();
    assertTrue(policy.isReadOnly());
    assertFalse(policy.isBestEffort());
    assertEquals(policy.getMaxRetries(), 5); // default
  }

  @Test
  public void testBestEffortFactory() {
    RetryPolicy policy = RetryPolicy.bestEffort();
    assertTrue(policy.isReadOnly()); // best-effort implies read-only
    assertTrue(policy.isBestEffort());
  }

  @Test
  public void testBuilderCustomValues() {
    RetryPolicy policy =
        RetryPolicy.builder()
            .maxRetries(10)
            .baseDelay(Duration.ofMillis(200))
            .maxDelay(Duration.ofSeconds(10))
            .jitter(0.5)
            .readOnly()
            .build();
    assertEquals(policy.getMaxRetries(), 10);
    assertEquals(policy.getBaseDelay(), Duration.ofMillis(200));
    assertEquals(policy.getMaxDelay(), Duration.ofSeconds(10));
    assertEquals(policy.getJitter(), 0.5);
    assertTrue(policy.isReadOnly());
  }

  @Test
  public void testBestEffortImpliesReadOnly() {
    RetryPolicy policy = RetryPolicy.builder().bestEffort().build();
    assertTrue(policy.isReadOnly());
    assertTrue(policy.isBestEffort());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeMaxRetries() {
    RetryPolicy.builder().maxRetries(-1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeBaseDelay() {
    RetryPolicy.builder().baseDelay(Duration.ofMillis(-1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeMaxDelay() {
    RetryPolicy.builder().maxDelay(Duration.ofMillis(-1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testJitterTooLow() {
    RetryPolicy.builder().jitter(-0.1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testJitterTooHigh() {
    RetryPolicy.builder().jitter(1.1);
  }

  @Test
  public void testExponentialBackoff() {
    RetryPolicy policy =
        RetryPolicy.builder()
            .baseDelay(Duration.ofMillis(100))
            .maxDelay(Duration.ofSeconds(5))
            .jitter(0) // disable jitter for deterministic test
            .build();

    assertEquals(policy.calculateDelay(0), 100);   // 100 * 2^0
    assertEquals(policy.calculateDelay(1), 200);   // 100 * 2^1
    assertEquals(policy.calculateDelay(2), 400);   // 100 * 2^2
    assertEquals(policy.calculateDelay(3), 800);   // 100 * 2^3
    assertEquals(policy.calculateDelay(4), 1600);  // 100 * 2^4
    assertEquals(policy.calculateDelay(5), 3200);  // 100 * 2^5
    assertEquals(policy.calculateDelay(6), 5000);  // capped at maxDelay
    assertEquals(policy.calculateDelay(10), 5000); // still capped
  }

  @Test
  public void testJitterAddsDelay() {
    RetryPolicy policy =
        RetryPolicy.builder()
            .baseDelay(Duration.ofMillis(1000))
            .maxDelay(Duration.ofSeconds(10))
            .jitter(0.5)
            .build();

    // With jitter=0.5, delay should be in range [1000, 1500] for attempt 0
    // Run multiple times to check bounds
    for (int i = 0; i < 100; i++) {
      long delay = policy.calculateDelay(0);
      assertTrue(delay >= 1000, "delay should be >= 1000, got " + delay);
      assertTrue(delay <= 1500, "delay should be <= 1500, got " + delay);
    }
  }

  @Test
  public void testZeroRetries() {
    RetryPolicy policy = RetryPolicy.builder().maxRetries(0).build();
    assertEquals(policy.getMaxRetries(), 0);
  }

  @Test
  public void testZeroJitter() {
    RetryPolicy policy = RetryPolicy.builder().jitter(0).build();
    assertEquals(policy.getJitter(), 0.0);
  }
}
