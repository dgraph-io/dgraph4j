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
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** @author Deepak Jois */
public class DgraphAsyncClientTest {
  private static final String TEST_HOSTNAME = "localhost";
  private static final int TEST_PORT = 9180;
  private DgraphAsyncClient dgraphAsyncClient;
  private ManagedChannel channel;

  @BeforeClass
  public void beforeClass() {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext().build();
    DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
    dgraphAsyncClient = new DgraphAsyncClient(stub);
    dgraphAsyncClient.login("groot", "password").join();
  }

  @AfterClass
  public void afterClass() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    dgraphAsyncClient.alter(Operation.newBuilder().setDropAll(true).build()).get();
  }

  @Test
  public void testDelete() throws Exception {
    try (AsyncTransaction txn = dgraphAsyncClient.newTransaction()) {
      Mutation setMutation =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();

      JsonParser parser = new JsonParser();
      String bobUid =
          txn.mutate(setMutation)
              .thenCompose(
                  ag -> {
                    String bob = ag.getUidsOrThrow("bob");
                    String query = String.format("{ find_bob(func: uid(%s)) { name } }", bob);
                    return txn.query(query)
                        .thenApply(
                            response -> {
                              JsonObject jsonData =
                                  parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
                              assertTrue(jsonData.getAsJsonArray("find_bob").size() > 0);
                              return bob;
                            });
                  })
              .get();

      Mutation delMutation =
          Mutation.newBuilder()
              .setDelNquads(ByteString.copyFromUtf8(String.format("<%s> <name> * .", bobUid)))
              .build();
      String query = String.format("{ find_bob(func: uid(%s)) { name } }", bobUid);
      txn.mutate(delMutation)
          .thenCompose(
              ag1 ->
                  txn.query(query)
                      .thenAccept(
                          response -> {
                            JsonObject jsonData =
                                parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
                            assertEquals(jsonData.getAsJsonArray("find_bob").size(), 0);
                          }))
          .get();
      txn.commit();
    }
  }

  @Test
  public void testNewTransactionFromContext() {
    DgraphProto.TxnContext ctx = DgraphProto.TxnContext.newBuilder().setStartTs(1234L).build();
    try (AsyncTransaction txn = dgraphAsyncClient.newTransaction(ctx)) {
      Response response = txn.query("{ result(func: uid(0x1)) { } }").join();
      assertEquals(response.getTxn().getStartTs(), 1234L);
    }
  }

  @Test
  public void testNewReadOnlyTransactionFromContext() {
    DgraphProto.TxnContext ctx = DgraphProto.TxnContext.newBuilder().setStartTs(1234L).build();
    try (AsyncTransaction txn = dgraphAsyncClient.newReadOnlyTransaction(ctx)) {
      Response response = txn.query("{ result(func: uid(0x1)) { } }").join();
      assertEquals(response.getTxn().getStartTs(), 1234L);
    }
  }

  @Test(expectedExceptions = TxnReadOnlyException.class)
  public void testMutationsInReadOnlyTransactions() {
    try (AsyncTransaction txn = dgraphAsyncClient.newReadOnlyTransaction()) {
      Mutation mu =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();
      txn.mutate(mu).join();
    }
  }

  @Test
  public void testQueryInReadOnlyTransactions() {
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) @upsert .").build();
    dgraphAsyncClient.alter(op).join();

    // Mutation
    JsonObject jsonData = new JsonObject();
    jsonData.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(jsonData.toString()))
            .build();
    dgraphAsyncClient.newTransaction().mutate(mu).join();

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response response =
        dgraphAsyncClient.newReadOnlyTransaction().queryWithVars(query, vars).join();

    // Verify data as expected
    JsonParser parser = new JsonParser();
    jsonData = parser.parse(response.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(jsonData.has("me"));
    String name = jsonData.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals(name, "Alice");
  }
}
