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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc.DgraphBlockingStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 */
public class DgraphClientTest {
  private static ManagedChannel channel;
  private static DgraphClient dgraphClient;

  private static final String TEST_HOSTNAME = "localhost";
  private static final int TEST_PORT = 9080;

  @BeforeClass
  public static void beforeClass() {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
    dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));
  }

  @Test
  public void textTxnQueryVariables() throws Exception {
    // Set schema
    Operation op = Operation.newBuilder().setSchema("name: string @index(exact) .").build();
    dgraphClient.alter(op);

    // Add data
    JsonObject json = new JsonObject();
    json.addProperty("name", "Alice");

    Mutation mu =
        Mutation.newBuilder()
            .setCommitImmediately(true)
            .setSetJson(ByteString.copyFromUtf8(json.toString()))
            .build();
    dgraphClient.newTransaction().mutate(mu);

    // Query
    String query = "{\n" + "me(func: eq(name, $a)) {\n" + "    name\n" + "  }\n" + "}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    Response res = dgraphClient.newTransaction().query(query, vars);

    // Verify data as expected
    JsonParser parser = new JsonParser();
    json = parser.parse(res.getJson().toStringUtf8()).getAsJsonObject();
    assertTrue(json.has("me"));
    String name = json.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    assertEquals("Alice", name);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
