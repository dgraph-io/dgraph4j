/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.DgraphProto.*;
import org.testng.annotations.Test;

public class NamespaceTest extends DgraphIntegrationTest {

  @Test
  public void testCreateNamespace() {
    CreateNamespaceResponse response = dgraphClient.createNamespace();
    long nsId = response.getNamespace();
    assertTrue(nsId > 0, "Namespace ID should be > 0, got: " + nsId);

    // Clean up
    dgraphClient.dropNamespace(nsId);
  }

  @Test
  public void testCreateMultipleNamespaces() {
    CreateNamespaceResponse resp1 = dgraphClient.createNamespace();
    CreateNamespaceResponse resp2 = dgraphClient.createNamespace();

    long ns1 = resp1.getNamespace();
    long ns2 = resp2.getNamespace();

    assertNotEquals(ns1, ns2, "Two namespaces should have different IDs");
    assertTrue(ns1 > 0);
    assertTrue(ns2 > 0);

    // Clean up
    dgraphClient.dropNamespace(ns1);
    dgraphClient.dropNamespace(ns2);
  }

  @Test
  public void testListNamespaces() {
    CreateNamespaceResponse created = dgraphClient.createNamespace();
    long nsId = created.getNamespace();

    try {
      ListNamespacesResponse listResp = dgraphClient.listNamespaces();
      assertTrue(
          listResp.getNamespacesMap().containsKey(nsId),
          "Listed namespaces should contain newly created namespace " + nsId);
    } finally {
      dgraphClient.dropNamespace(nsId);
    }
  }

  @Test
  public void testDropNamespace() {
    CreateNamespaceResponse created = dgraphClient.createNamespace();
    long nsId = created.getNamespace();

    // Verify it exists
    ListNamespacesResponse beforeDrop = dgraphClient.listNamespaces();
    assertTrue(
        beforeDrop.getNamespacesMap().containsKey(nsId),
        "Namespace should exist before drop");

    // Drop it
    dgraphClient.dropNamespace(nsId);

    // Namespace deletion can be eventually consistent, so retry
    waitForNamespaceDeletion(nsId, 10, 500);
  }

  @Test
  public void testCreateAndDropMultipleNamespaces() {
    CreateNamespaceResponse resp1 = dgraphClient.createNamespace();
    CreateNamespaceResponse resp2 = dgraphClient.createNamespace();
    CreateNamespaceResponse resp3 = dgraphClient.createNamespace();

    long ns1 = resp1.getNamespace();
    long ns2 = resp2.getNamespace();
    long ns3 = resp3.getNamespace();

    // Verify all exist
    ListNamespacesResponse listed = dgraphClient.listNamespaces();
    assertTrue(listed.getNamespacesMap().containsKey(ns1));
    assertTrue(listed.getNamespacesMap().containsKey(ns2));
    assertTrue(listed.getNamespacesMap().containsKey(ns3));

    // Drop all
    dgraphClient.dropNamespace(ns1);
    dgraphClient.dropNamespace(ns2);
    dgraphClient.dropNamespace(ns3);

    // Verify all gone
    waitForNamespaceDeletion(ns1, 10, 500);
    waitForNamespaceDeletion(ns2, 10, 500);
    waitForNamespaceDeletion(ns3, 10, 500);
  }

  private void waitForNamespaceDeletion(long nsId, int maxRetries, long initialDelayMs) {
    long delay = initialDelayMs;
    for (int i = 0; i < maxRetries; i++) {
      ListNamespacesResponse resp = dgraphClient.listNamespaces();
      if (!resp.getNamespacesMap().containsKey(nsId)) {
        return; // Namespace is gone
      }
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted while waiting for namespace deletion");
      }
      delay = Math.min(delay * 2, 5000); // Exponential backoff, max 5s
    }
    fail("Namespace " + nsId + " still exists after " + maxRetries + " retries");
  }
}
