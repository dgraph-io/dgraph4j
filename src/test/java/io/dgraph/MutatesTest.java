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

import io.dgraph.DgraphProto.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class MutatesTest extends DgraphIntegrationTest {

  private static String[] data = new String[] {"200", "300", "400"};

  private static Map<String, String> uidsMap;

  @Test
  public void testInsert3Quads() throws Exception {
    Operation op =
        Operation.newBuilder().setSchema("name: string @index(fulltext) @upsert .").build();
    dgraphClient.alter(op);

    Transaction txn = dgraphClient.newTransaction();

    uidsMap = new HashMap<>();
    for (String datum : data) {
      NQuad quad =
          NQuad.newBuilder()
              .setSubject(String.format("_:%s", datum))
              .setPredicate("name")
              .setObjectValue(Value.newBuilder().setStrVal(String.format("ok %s", datum)).build())
              .build();

      Mutation mu = Mutation.newBuilder().addSet(quad).build();

      Response resp = txn.mutate(mu);
      uidsMap.put(datum, resp.getUidsOrThrow(datum));
    }

    txn.commit();
    logger.debug("Commit Ok");
  }

  @Test
  public void testQuery3Quads() throws Exception {
    Transaction txn = dgraphClient.newTransaction();
    List<String> uids = Arrays.stream(data).map(d -> uidsMap.get(d)).collect(Collectors.toList());

    String query = String.format("{ me(func: uid(%s)) { name }}", String.join(",", uids));
    logger.debug("Query: {}\n", query);
    Response response = txn.query(query);
    String res = response.getJson().toStringUtf8();
    logger.debug("Response JSON: {}\n", res);

    String exp = "{\"me\":[{\"name\":\"ok 200\"},{\"name\":\"ok 300\"},{\"name\":\"ok 400\"}]}";
    assertEquals(res, exp);
    assertTrue(response.getTxn().getStartTs() > 0);
    txn.commit();
  }
}
