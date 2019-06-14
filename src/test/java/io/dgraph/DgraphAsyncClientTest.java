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
  private ManagedChannel channel;
  protected DgraphAsyncClient dgraphAsyncClient;

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;

  @BeforeClass
  public void beforeClass() {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
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
      Mutation mutation =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();

      JsonParser parser = new JsonParser();
      String bobUid =
          txn.mutate(mutation)
              .thenCompose(
                  ag -> {
                    String bob = ag.getUidsOrThrow("bob");
                    String query = String.format("{ find_bob(func: uid(%s)) { name } }", bob);
                    return txn.query(query)
                        .thenApply(
                            resp -> {
                              JsonObject json =
                                  parser.parse(resp.getJson().toStringUtf8()).getAsJsonObject();
                              assertTrue(json.getAsJsonArray("find_bob").size() > 0);
                              return bob;
                            });
                  })
              .get();

      Mutation mutation1 =
          Mutation.newBuilder()
              .setDelNquads(ByteString.copyFromUtf8(String.format("<%s> <name> * .", bobUid)))
              .build();
      String query = String.format("{ find_bob(func: uid(%s)) { name } }", bobUid);
      txn.mutate(mutation1)
          .thenCompose(
              ag1 ->
                  txn.query(query)
                      .thenAccept(
                          resp1 -> {
                            JsonObject json1 =
                                parser.parse(resp1.getJson().toStringUtf8()).getAsJsonObject();
                            assertTrue(json1.getAsJsonArray("find_bob").size() == 0);
                          }))
          .get();

      txn.commit();
    }
  }

  @Test(expectedExceptions = TxnReadOnlyException.class)
  public void testMutationsInReadOnlyTransactions() {
    try (AsyncTransaction txn = dgraphAsyncClient.newReadOnlyTransaction()) {
      Mutation mutation =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();

      txn.mutate(mutation).join();
    }
  }

  @Test
  public void testQueryInReadOnlyTransactions() {
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) @upsert .").build();
    dgraphAsyncClient.alter(op).join();

    // Add data
    JsonObject json = new JsonObject();
    json.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitNow(true)
            .setSetJson(ByteString.copyFromUtf8(json.toString()))
            .build();
    dgraphAsyncClient.newTransaction().mutate(mu).join();

    // Query
    String query = "query me($a: string) { me(func: eq(name, $a)) { name }}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response res = dgraphAsyncClient.newReadOnlyTransaction().queryWithVars(query, vars).join();

    // Verify data as expected
    JsonParser parser = new JsonParser();
    json = parser.parse(res.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(json.has("me"));
    String name = json.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals(name, "Alice");
  }
}
