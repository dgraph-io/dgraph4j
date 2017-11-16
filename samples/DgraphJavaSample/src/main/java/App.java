import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphBlockingStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.DgraphClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class App {
  private static final String TEST_HOSTNAME = "localhost";
  private static final int TEST_PORT = 9080;

  public static void main(final String[] args) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
    DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
    DgraphClient dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));

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
    String name = json.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
    System.out.println(name);
  }
}
