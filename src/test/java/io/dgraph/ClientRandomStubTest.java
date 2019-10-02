package io.dgraph;

import io.grpc.Channel;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ClientRandomStubTest extends DgraphIntegrationTest {
  private Field asyncTransactionField, stubField, channelField;

  public ClientRandomStubTest() throws NoSuchFieldException, ClassNotFoundException {
    asyncTransactionField =
        Class.forName("io.dgraph.Transaction").getDeclaredField("asyncTransaction");
    asyncTransactionField.setAccessible(true);
    stubField = Class.forName("io.dgraph.AsyncTransaction").getDeclaredField("stub");
    stubField.setAccessible(true);
    channelField = Class.forName("io.grpc.stub.AbstractStub").getDeclaredField("channel");
    channelField.setAccessible(true);
  }

  @Test
  public void testClientRandomStubTest() throws IllegalAccessException {
    int NUM_ITER = 1000;
    HashMap<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < NUM_ITER; i++) {
      Transaction txn = dgraphClient.newTransaction();
      Channel channel = (Channel) channelField.get(stubField.get(asyncTransactionField.get(txn)));
      String endpoint = channel.authority();
      if (counts.containsKey(endpoint)) {
        counts.put(endpoint, counts.get(endpoint) + 1);
      } else {
        counts.put(endpoint, 1);
      }
    }

    // Ensure that we got all the clients
    assertEquals(counts.size(), 3);
    int sum = 0;
    for (Map.Entry<String, Integer> ep : counts.entrySet()) {
      assertTrue(ep.getValue() > 300);
      sum += ep.getValue();
    }

    assertEquals(sum, NUM_ITER);
  }
}
