package io.dgraph;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphProto.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class MutatesTest extends DgraphIntegrationTest {

  private static String[] data = new String[] {"200", "300", "400"};

  private static Map<String, String> uidsMap;

  @Test
  public void testInsert3Quads() throws Exception {
    Operation op = Operation.newBuilder().setSchema("name: string @index(fulltext) .").build();
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

      Assigned ag = txn.mutate(mu);
      uidsMap.put(datum, ag.getUidsOrThrow(datum));
    }

    txn.commit();
    logger.debug("Commit Ok");
  }

  @Test
  public void testQuery3Quads() throws Exception {
    Transaction txn = dgraphClient.newTransaction();
    List<String> uids =
        Arrays.asList(data).stream().map(d -> uidsMap.get(d)).collect(Collectors.toList());

    String query = String.format("{ me(func: uid(%s)) { name }}", String.join(",", uids));
    logger.debug("Query: {}\n", query);
    Response response = txn.query(query);
    String res = response.getJson().toStringUtf8();
    logger.debug("Response JSON: {}\n", res);

    String exp = "{\"me\":[{\"name\":\"ok 200\"},{\"name\":\"ok 300\"},{\"name\":\"ok 400\"}]}";
    assertEquals(exp, res);
    assertTrue(response.getTxn().getStartTs() > 0);
    txn.commit();
  }
}
