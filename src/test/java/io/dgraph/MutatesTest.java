package io.dgraph;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import io.dgraph.DgraphClient.Transaction;
import io.dgraph.DgraphProto.*;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MutatesTest extends DgraphIntegrationTest {

  private static String[] data = new String[] {"200", "300", "400"};

  @Test
  public void testInsert3Quads() throws Exception {
    Operation op = Operation.newBuilder().setSchema("name: string @index(fulltext) .").build();
    dgraphClient.alter(op);

    Transaction txn = dgraphClient.newTransaction();

    for (String datum : data) {
      NQuad quad =
          NQuad.newBuilder()
              .setSubject(datum)
              .setPredicate("name")
              .setObjectValue(Value.newBuilder().setStrVal(String.format("ok %s", datum)).build())
              .build();

      Mutation mu = Mutation.newBuilder().addSet(quad).build();

      txn.mutate(mu);
    }

    txn.commit();
    logger.debug("Commit Ok");
  }

  @Test
  public void testQuery3Quads() throws Exception {
    Transaction txn = dgraphClient.newTransaction();
    String query = String.format("{ me(func: uid(%s)) { name }}", String.join(",", data));
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
