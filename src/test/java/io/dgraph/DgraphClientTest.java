/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.*;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 */
public class DgraphClientTest extends DgraphIntegrationTest {

  @BeforeMethod
  public void beforeMethod() {
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @Test
  public void testTxnQueryVariables() {
    // Set schema
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) @upsert .").build();
    dgraphClient.alter(op);

    // Add data
    JsonObject json = new JsonObject();
    json.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(json.toString()))
            .build();
    dgraphClient.newTransaction().mutate(mu);

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response res = dgraphClient.newTransaction().queryWithVars(query, vars);

    // Verify data as expected
    JsonParser parser = new JsonParser();
    json = parser.parse(res.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(json.has("me"));
    String name = json.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals("Alice", name);
  }

  @Test
  public void testDelete() {
    try (Transaction txn = dgraphClient.newTransaction()) {

      Mutation mutation =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();
      Assigned ag = txn.mutate(mutation);
      String bob = ag.getUidsOrThrow("bob");

      JsonParser parser = new JsonParser();
      String query = String.format("{ find_bob(func: uid(%s)) { name } }", bob);
      Response resp = txn.query(query);
      JsonObject json = parser.parse(resp.getJson().toStringUtf8()).getAsJsonObject();
      assertTrue(json.getAsJsonArray("find_bob").size() > 0);

      mutation =
          Mutation.newBuilder()
              .setDelNquads(ByteString.copyFromUtf8(String.format("<%s> * * .", bob)))
              .build();
      txn.mutate(mutation);

      resp = txn.query(query);
      json = parser.parse(resp.getJson().toStringUtf8()).getAsJsonObject();
      assertTrue(json.getAsJsonArray("find_bob").size() == 0);

      txn.commit();
    }
  }

  @Test(expectedExceptions = TxnWrongStateException.class)
  public void testCommitAfterCommitNow() {
    try (Transaction txn = dgraphClient.newTransaction()) {

      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
      txn.commit();
    }
  }

  @Test
  public void testDiscardAbort() {
    try (Transaction txn = dgraphClient.newTransaction()) {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .setCommitNow(true)
              .build();
      txn.mutate(mu);
    }
  }
}
