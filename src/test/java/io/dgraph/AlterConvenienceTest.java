/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.DgraphProto.*;
import java.time.Duration;
import org.testng.annotations.Test;

public class AlterConvenienceTest extends DgraphIntegrationTest {
  private static final Duration SCHEMA_PROPAGATION_TIMEOUT = Duration.ofSeconds(30);
  private static final long SCHEMA_POLL_INTERVAL_MS = 250;

  @Test
  public void testDropAll() {
    // Set schema and add data
    dgraphClient.setSchema("name: string @index(exact) .");
    Transaction txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:alice <name> \"Alice\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    // Drop all
    dgraphClient.dropAll();

    // Verify data is gone - querying without schema should return empty
    // Re-set schema to be able to query
    dgraphClient.setSchema("name: string @index(exact) .");
    String query = "{ q(func: eq(name, \"Alice\")) { name } }";
    Response response = dgraphClient.newReadOnlyTransaction().query(query);
    String json = response.getJson().toStringUtf8();
    assertFalse(json.contains("Alice"), "Data should be gone after dropAll: " + json);
  }

  @Test
  public void testDropData() {
    // Set schema and add data
    dgraphClient.setSchema("name: string @index(exact) .");
    Transaction txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:bob <name> \"Bob\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    // Drop data only (preserves schema)
    dgraphClient.dropData();

    // Verify data is gone but we can still query (schema preserved)
    String query = "{ q(func: eq(name, \"Bob\")) { name } }";
    Response response = dgraphClient.newReadOnlyTransaction().query(query);
    String json = response.getJson().toStringUtf8();
    assertFalse(json.contains("Bob"), "Data should be gone after dropData: " + json);

    // Verify we can still insert data (schema is preserved)
    txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:carol <name> \"Carol\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    response = dgraphClient.newReadOnlyTransaction().query(
        "{ q(func: eq(name, \"Carol\")) { name } }");
    json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Carol"), "Should be able to add data after dropData: " + json);
  }

  @Test
  public void testDropPredicate() {
    // Set schema with multiple predicates
    dgraphClient.setSchema(
        "name: string @index(exact) .\n"
            + "age: int .\n"
            + "email: string @index(exact) .");

    // Add data
    Transaction txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:dave <name> \"Dave\" .\n"
                          + "_:dave <age> \"30\" .\n"
                          + "_:dave <email> \"dave@example.com\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    // Drop the "age" predicate
    dgraphClient.dropPredicate("age");

    // Verify age is gone but name and email remain
    String query = "{ q(func: eq(name, \"Dave\")) { name age email } }";
    Response response = dgraphClient.newReadOnlyTransaction().query(query);
    String json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Dave"), "Name should remain: " + json);
    assertTrue(json.contains("dave@example.com"), "Email should remain: " + json);
    assertFalse(json.contains("\"age\""), "Age should be gone: " + json);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDropPredicateEmptyString() {
    dgraphClient.dropPredicate("");
  }

  @Test
  public void testDropType() {
    // Clean slate: previous tests may leave predicates that cause Dgraph to
    // skip the alter when no predicate changes are detected, silently
    // ignoring the type definition.
    dgraphClient.dropAll();

    // Set schema with a type definition
    dgraphClient.setSchema(
        "name: string @index(exact) .\n" + "type Person {\n" + "  name\n" + "}");

    awaitType("Person", true);

    // Drop the type
    dgraphClient.dropType("Person");

    awaitType("Person", false);
  }

  /**
   * Polls {@code schema { types }} until {@code type} is present or absent as required.
   *
   * <p>An alter reaches each alpha asynchronously and every query picks an alpha at random, so a
   * single read after an alter observes the old schema whenever it lands on an alpha that has not
   * caught up yet.
   */
  private void awaitType(String type, boolean shouldExist) {
    String json = "";
    long deadline = System.nanoTime() + SCHEMA_PROPAGATION_TIMEOUT.toNanos();
    do {
      json =
          dgraphClient.newReadOnlyTransaction().query("schema { types }").getJson().toStringUtf8();
      if (json.contains(type) == shouldExist) {
        return;
      }
      try {
        Thread.sleep(SCHEMA_POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted while waiting for the schema to propagate");
      }
    } while (System.nanoTime() < deadline);

    fail(
        "Type "
            + type
            + (shouldExist ? " should exist in" : " should be gone from")
            + " the schema after "
            + SCHEMA_PROPAGATION_TIMEOUT.getSeconds()
            + "s: "
            + json);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDropTypeEmptyString() {
    dgraphClient.dropType("");
  }

  @Test
  public void testSetSchema() {
    // Set initial schema
    dgraphClient.setSchema("name: string @index(exact) .\nage: int .");

    // Verify we can use the schema
    Transaction txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:frank <name> \"Frank\" .\n"
                          + "_:frank <age> \"25\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    String query = "{ q(func: eq(name, \"Frank\")) { name age } }";
    Response response = dgraphClient.newReadOnlyTransaction().query(query);
    String json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Frank"), "Should find Frank: " + json);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetSchemaEmptyString() {
    dgraphClient.setSchema("");
  }
}
