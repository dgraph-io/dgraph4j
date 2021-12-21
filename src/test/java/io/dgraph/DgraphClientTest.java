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

import static org.testng.Assert.*;
import static org.testng.Assert.fail;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.DgraphProto.TxnContext;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    JsonObject data = new JsonObject();
    data.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(data.toString()))
            .build();
    dgraphClient.newTransaction().mutate(mu);

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response response = dgraphClient.newTransaction().queryWithVars(query, vars);

    // Verify data as expected
    JsonParser parser = new JsonParser();
    data = parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(data.has("me"));
    String name = data.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals("Alice", name);
  }

  @Test
  public void testTxnQueryRDFWithVariables() {
    // Set schema
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) @upsert .").build();
    dgraphClient.alter(op);

    // Add data
    JsonObject data = new JsonObject();
    data.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(data.toString()))
            .build();
    Response muRes = dgraphClient.newTransaction().mutate(mu);

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response response = dgraphClient.newTransaction().queryRDFWithVars(query, vars);

    // Verify data as expected
    assertEquals(muRes.getUidsMap().values().size(), 1);
    String uid = (String) muRes.getUidsMap().values().toArray()[0];
    assertEquals(response.getRdf().toStringUtf8(), "<" + uid + "> <name> \"Alice\" .\n");
  }

  @Test
  public void testDelete() {
    try (Transaction txn = dgraphClient.newTransaction()) {

      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();
      Response response = txn.mutate(mu);
      String bob = response.getUidsOrThrow("bob");

      JsonParser parser = new JsonParser();
      String query = String.format("{ find_bob(func: uid(%s)) { name } }", bob);
      response = txn.query(query);
      JsonObject jsonData = parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
      assertTrue(jsonData.getAsJsonArray("find_bob").size() > 0);

      mu =
          Mutation.newBuilder()
              .setDelNquads(ByteString.copyFromUtf8(String.format("<%s> <name> * .", bob)))
              .build();
      txn.mutate(mu);

      response = txn.query(query);
      jsonData = parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
      assertEquals(jsonData.getAsJsonArray("find_bob").size(), 0);
      txn.commit();
    }
  }

  @Test
  public void testNewTransactionFromContext() {
    TxnContext ctx = TxnContext.newBuilder().build();
    try (Transaction txn = dgraphClient.newTransaction(ctx)) {
      Response response = txn.query("{ result(func: uid(0x1)) { } }");
      assertTrue(response.getTxn().getStartTs() > 0L);
    }
  }

  @Test
  public void testNewReadOnlyTransactionFromContext() {
    TxnContext ctx = TxnContext.newBuilder().build();
    try (Transaction txn = dgraphClient.newReadOnlyTransaction(ctx)) {
      Response response = txn.query("{ result(func: uid(0x1)) { } }");
      assertTrue(response.getTxn().getStartTs() > 0L);
    }
  }

  @Test(expectedExceptions = TxnFinishedException.class)
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

  @Test
  public void testCheckVersion() {
    DgraphProto.Version v = dgraphClient.checkVersion();
    assertTrue(v.getTag().length() > 0);
    assertEquals(v.getTag().charAt(0), 'v');
  }

  @Test
  public void testFromCloudEndpoint_ValidURL() {
    try {
      DgraphClient.clientStubFromCloudEndpoint("https://your-instance.cloud.dgraph.io/graphql", "");
    } catch (MalformedURLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFromCloudEndpoint_InValidURL() {
    try {
      DgraphClient.clientStubFromCloudEndpoint("https://a-bad-url", "");
      fail("Invalid Slash URL should not be accepted.");
    } catch (MalformedURLException e) {
    }
  }

  @Test
  public void testTimeouts() {
    // Set schema
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) @upsert .").build();
    dgraphClient.alter(op);

    // Add data
    JsonObject data = new JsonObject();
    data.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(data.toString()))
            .build();
    dgraphClient.newTransaction().mutate(mu, 10, TimeUnit.SECONDS);

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response response =
        dgraphClient.newTransaction().queryWithVars(query, vars, 10, TimeUnit.SECONDS);

    // Verify data as expected
    JsonParser parser = new JsonParser();
    data = parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(data.has("me"));
    String name = data.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals("Alice", name);
  }
}
