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
import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphProto.LinRead;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * @author Edgar Rodriguez-Diaz
 * @author Deepak Jois
 */
public class DgraphClientTest extends DgraphIntegrationTest {

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

  @Test
  public void testInvalidUtf8() throws Exception {
    // Initialize
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());

    Transaction txn = dgraphClient.newTransaction();
    try {
      for (int i = 0; i < 1000; i++) {
        JsonObject json = new JsonObject();
        json.addProperty("id", i);
        json.addProperty("name", "abcdefgh" + i);

        Mutation mu =
            Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json.toString())).build();
        txn = dgraphClient.newTransaction();
        txn.mutate(mu);
      }
      txn.commit();
    } finally {
      txn.discard();
    }
  }
}
