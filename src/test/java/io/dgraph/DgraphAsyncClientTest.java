package io.dgraph;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DgraphAsyncClientTest {
  private static ManagedChannel channel;
  private static DgraphAsyncClient dgraphClient;

  private static final String TEST_HOSTNAME = "localhost";
  private static final int TEST_PORT = 9080;

  @BeforeClass
  public static void beforeClass() {
    channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphGrpc.DgraphFutureStub blockingStub = DgraphGrpc.newFutureStub(channel);
    dgraphClient = new DgraphAsyncClient(Collections.singletonList(blockingStub));
  }

  @Test
  public void textTxnQueryVariables() throws Exception {
    // Set schema
    DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema("name: string @index(exact) .").build();
    dgraphClient.alter(op);

    // Add data
    JsonObject json = new JsonObject();
    json.addProperty("name", "Alice");

    DgraphProto.Mutation mu =
        DgraphProto.Mutation.newBuilder()
            .setCommitImmediately(true)
            .setSetJson(ByteString.copyFromUtf8(json.toString()))
            .build();
    dgraphClient.newTransaction().mutateAsync(mu).get(3, TimeUnit.SECONDS);

    // Query
    String query = "{\n" + "me(func: eq(name, $a)) {\n" + "    name\n" + "  }\n}";
    Map<String, String> vars = Collections.singletonMap("$a", "Alice");
    DgraphProto.Response res = dgraphClient.newTransaction().queryAsync(query, vars).get(3, TimeUnit.SECONDS);

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