/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import io.dgraph.DgraphProto.*;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

public class RunDQLTest extends DgraphIntegrationTest {

  @Test
  public void testRunDQLMutationAndQuery() {
    // Set schema first
    dgraphClient.alter(
        Operation.newBuilder().setSchema("name: string @index(exact) .").build());

    // Run a DQL mutation via runDQL
    String mutation =
        "{\n"
            + "  set {\n"
            + "    _:alice <name> \"Alice\" .\n"
            + "    _:alice <dgraph.type> \"Person\" .\n"
            + "  }\n"
            + "}";
    dgraphClient.runDQL(mutation);

    // Query to verify
    String query = "{ q(func: eq(name, \"Alice\")) { name } }";
    Response response = dgraphClient.runDQL(query);
    String json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Alice"), "Expected 'Alice' in response: " + json);
  }

  @Test
  public void testRunDQLWithVars() {
    dgraphClient.alter(
        Operation.newBuilder().setSchema("name: string @index(exact) .").build());

    // Insert data
    String mutation =
        "{\n"
            + "  set {\n"
            + "    _:bob <name> \"Bob\" .\n"
            + "    _:bob <dgraph.type> \"Person\" .\n"
            + "  }\n"
            + "}";
    dgraphClient.runDQL(mutation);

    // Query with variables
    String query = "query q($name: string) { q(func: eq(name, $name)) { name } }";
    Map<String, String> vars = Collections.singletonMap("$name", "Bob");
    Response response = dgraphClient.runDQL(query, vars, true, false);
    String json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Bob"), "Expected 'Bob' in response: " + json);
  }

  @Test
  public void testRunDQLReadOnly() {
    dgraphClient.alter(
        Operation.newBuilder().setSchema("name: string @index(exact) .").build());

    // Insert data using a regular transaction
    Transaction txn = dgraphClient.newTransaction();
    try {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(
                  com.google.protobuf.ByteString.copyFromUtf8(
                      "_:charlie <name> \"Charlie\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    } finally {
      txn.discard();
    }

    // Query with readOnly flag
    String query = "{ q(func: eq(name, \"Charlie\")) { name } }";
    Response response =
        dgraphClient.runDQL(query, Collections.emptyMap(), true, false);
    String json = response.getJson().toStringUtf8();
    assertTrue(json.contains("Charlie"), "Expected 'Charlie' in response: " + json);
  }
}
