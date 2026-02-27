/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.DgraphProto.AllocateIDsResponse;
import org.testng.annotations.Test;

public class AllocateIDsTest extends DgraphIntegrationTest {

  @Test
  public void testAllocateUIDs() {
    AllocateIDsResponse response = dgraphClient.allocateUIDs(100);
    assertTrue(response.getStart() > 0, "Start should be > 0");
    assertTrue(response.getEnd() > response.getStart(), "End should be > start");
    assertEquals(response.getEnd() - response.getStart() + 1, 100);
  }

  @Test
  public void testAllocateUIDsTwice() {
    AllocateIDsResponse first = dgraphClient.allocateUIDs(50);
    AllocateIDsResponse second = dgraphClient.allocateUIDs(50);

    assertTrue(first.getStart() > 0);
    assertTrue(second.getStart() > 0);
    // Second allocation should not overlap with first
    assertTrue(
        second.getStart() > first.getEnd(),
        "Second allocation should start after first ends");
  }

  @Test
  public void testAllocateTimestamps() {
    AllocateIDsResponse response = dgraphClient.allocateTimestamps(50);
    assertTrue(response.getStart() > 0, "Start should be > 0");
    assertTrue(response.getEnd() >= response.getStart(), "End should be >= start");
  }

  @Test
  public void testAllocateNamespaces() {
    AllocateIDsResponse response = dgraphClient.allocateNamespaces(10);
    assertTrue(response.getStart() > 0, "Start should be > 0");
    assertTrue(response.getEnd() >= response.getStart(), "End should be >= start");
  }

  @Test
  public void testAllocateUIDsDifferentSizes() {
    AllocateIDsResponse small = dgraphClient.allocateUIDs(1);
    AllocateIDsResponse large = dgraphClient.allocateUIDs(1000);

    assertTrue(small.getStart() > 0);
    assertEquals(small.getEnd() - small.getStart() + 1, 1);

    assertTrue(large.getStart() > 0);
    assertEquals(large.getEnd() - large.getStart() + 1, 1000);

    // Non-overlapping
    assertTrue(
        large.getStart() > small.getEnd(),
        "Large allocation should start after small ends");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAllocateZeroItems() {
    dgraphClient.allocateUIDs(0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAllocateNegativeItems() {
    dgraphClient.allocateTimestamps(-1);
  }

  @Test
  public void testAllocationTypesIndependent() {
    AllocateIDsResponse uids = dgraphClient.allocateUIDs(10);
    AllocateIDsResponse timestamps = dgraphClient.allocateTimestamps(10);
    AllocateIDsResponse namespaces = dgraphClient.allocateNamespaces(10);

    // All should return valid ranges
    assertTrue(uids.getStart() > 0);
    assertTrue(timestamps.getStart() > 0);
    assertTrue(namespaces.getStart() > 0);
  }
}
