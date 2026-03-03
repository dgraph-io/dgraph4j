/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc. <dgraph-admin@istaridigital.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.DgraphProto.*;
import org.testng.annotations.Test;

public class AlterConvenienceTest extends DgraphIntegrationTest {

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
    // Set schema with a type definition
    dgraphClient.setSchema(
        "name: string @index(exact) .\n" + "type Person {\n" + "  name\n" + "}");

    // Verify the type exists in the schema by querying all types
    String schemaQuery = "schema { types }";
    Response schemaBefore =
        dgraphClient.newReadOnlyTransaction().query(schemaQuery);
    String schemaBJson = schemaBefore.getJson().toStringUtf8();
    assertTrue(
        schemaBJson.contains("Person"),
        "Type Person should exist in schema before drop: " + schemaBJson);

    // Drop the type
    dgraphClient.dropType("Person");

    // Verify the type definition is gone from the schema
    Response schemaAfter =
        dgraphClient.newReadOnlyTransaction().query(schemaQuery);
    String schemaAJson = schemaAfter.getJson().toStringUtf8();
    assertFalse(
        schemaAJson.contains("Person"),
        "Type Person should be gone from schema after drop: " + schemaAJson);
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
