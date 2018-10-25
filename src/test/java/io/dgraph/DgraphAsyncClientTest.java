/*
 * Copyright (C) 2017-18 Dgraph Labs, Inc. and Contributors
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

import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Deepak Jois */
public class DgraphAsyncClientTest {
  private static ManagedChannel channel;
  protected static DgraphAsyncClient dgraphAsyncClient;

  protected static final String TEST_HOSTNAME = "localhost";
  protected static final int TEST_PORT = 9180;

  @BeforeClass
  public static void beforeClass() throws Exception {

    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
    dgraphAsyncClient = new DgraphAsyncClient(stub);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Before
  public void beforeTest() throws Exception {
    dgraphAsyncClient.alter(Operation.newBuilder().setDropAll(true).build()).get();
  }

  @Test
  public void testDelete() throws Exception {
    try (AsyncTransaction txn = dgraphAsyncClient.newTransaction()) {
      Mutation mutation =
          Mutation.newBuilder()
              .setSetNquads(ByteString.copyFromUtf8("<_:bob> <name> \"Bob\" ."))
              .build();

      txn.mutate(mutation)
          .thenCompose(
              ag -> {
                String bob = ag.getUidsOrThrow("bob");
                JsonParser parser = new JsonParser();
                String query = String.format("{ find_bob(func: uid(%s)) { name } }", bob);
                return txn.query(query)
                    .thenCompose(
                        resp -> {
                          JsonObject json =
                              parser.parse(resp.getJson().toStringUtf8()).getAsJsonObject();
                          assertTrue(json.getAsJsonArray("find_bob").size() > 0);
                          Mutation mutation1 =
                              Mutation.newBuilder()
                                  .setDelNquads(
                                      ByteString.copyFromUtf8(String.format("<%s> * * .", bob)))
                                  .build();
                          return txn.mutate(mutation1)
                              .thenCompose(
                                  ag1 ->
                                      txn.query(query)
                                          .thenAccept(
                                              resp1 -> {
                                                JsonObject json1 =
                                                    parser
                                                        .parse(resp1.getJson().toStringUtf8())
                                                        .getAsJsonObject();
                                                assertTrue(
                                                    json1.getAsJsonArray("find_bob").size() == 0);
                                              }));
                        });
              })
          .get();

      txn.commit();
    }
  }
}
