/*
 * Copyright 2016-17 DGraph Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dgraph;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 */
public class DgraphClientTest extends DgraphIntegrationTest {

  @Before
  public void beforeTest() {
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @Test
  public void testMergeContext() throws Exception {
    LinRead dst =
        LinRead.newBuilder()
            .putAllIds(
                new HashMap<Integer, Long>() {
                  {
                    put(1, 10L);
                    put(2, 15L);
                    put(3, 10L);
                  }
                })
            .build();

    LinRead src =
        LinRead.newBuilder()
            .putAllIds(
                new HashMap<Integer, Long>() {
                  {
                    put(2, 10L);
                    put(3, 15L);
                    put(4, 10L);
                  }
                })
            .build();

    LinRead result = dgraphClient.mergeLinReads(dst, src);

    assertEquals(4, result.getIdsCount());
    assertEquals(10L, result.getIdsOrThrow(1));
    assertEquals(15L, result.getIdsOrThrow(2));
    assertEquals(15L, result.getIdsOrThrow(3));
    assertEquals(10L, result.getIdsOrThrow(4));
  }

  @Test
  public void testTxnQueryVariables() throws Exception {
    // Set schema
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) .").build();
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
  public void testDelete() throws Exception {
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

  @Test(expected = TxnFinishedException.class)
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
  public void testClientWithDeadline() throws Exception {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
    dgraphClient = new DgraphClient(Collections.singletonList(blockingStub), 1);

    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) .").build();

    // Alters schema without exceeding the given deadline.
    dgraphClient.alter(op);

    // Creates a blocking stub directly, in order to force a deadline to be exceeded.
    Method method = DgraphClient.class.getDeclaredMethod("anyClient");
    method.setAccessible(true);

    DgraphGrpc.DgraphBlockingStub client =
        (DgraphGrpc.DgraphBlockingStub) method.invoke(dgraphClient, null);

    Thread.sleep(1001);

    try {
      client.alter(op);
      fail("Deadline should have been exceeded");
    } catch (StatusRuntimeException sre) {
      // Expected.
    }
  }
}
