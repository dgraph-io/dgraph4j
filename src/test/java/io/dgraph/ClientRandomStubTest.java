package io.dgraph;

import static org.testng.Assert.assertEquals;

import io.grpc.Channel;
import java.lang.reflect.Field;
import java.util.HashMap;
import org.testng.annotations.Test;

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
    HashMap<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < 100; i++) {
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
  }
}
