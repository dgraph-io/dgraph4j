import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.net.MalformedURLException;
import java.sql.Time;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class App {
  private static final String TEST_HOSTNAME = "localhost";
  private static final int TEST_PORT = 9180;
  private static final String TEST_CLOUD_ENDPOINT = System.getenv("TEST_CLOUD_ENDPOINT");
  private static final String TEST_CLOUD_API_KEY = System.getenv("TEST_CLOUD_API_KEY");

  private static DgraphClient createDgraphClient() {
    DgraphStub stub = null;
    try {
      stub = DgraphClient.clientStubFromCloudEndpoint(TEST_CLOUD_ENDPOINT, TEST_CLOUD_API_KEY);
      stub = stub.withDeadlineAfter(5, TimeUnit.SECONDS);
    } catch (MalformedURLException exception) {
      System.out.println("Error");
    }
    return new DgraphClient(stub);
  }

  public static void main(final String[] args) {
    System.out.println("Connecting to endpoint: " + TEST_CLOUD_ENDPOINT);
    DgraphClient dgraphClient = createDgraphClient();
    dgraphClient.login("groot", "password");

    // Initialize
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());

    // Set schema
    String schema = "name: string @index(exact) .";
    Operation operation = Operation.newBuilder().setSchema(schema).build();
    dgraphClient.alter(operation);

    Gson gson = new Gson(); // For JSON encode/decode
    Transaction txn = dgraphClient.newTransaction();
    try {
      // Create data
      Person p = new Person();
      p.name = "Alice";

      // Serialize it
      String json = gson.toJson(p);

      // Run mutation
      Mutation mutation =
          Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json.toString())).build();
      txn.mutate(mutation);
      txn.commit();

    } finally {
      txn.discard();
    }
    // Query
    for (int i =0 ; i < 10; i++) {
      String query =
              "query all($a: string){\n" + "all(func: eq(name, $a)) {\n" + "    name\n" + "  }\n" + "}";
      Map<String, String> vars = Collections.singletonMap("$a", "Alice");
      Response res = dgraphClient.newTransaction().queryWithVars(query, vars);

      // Deserialize
      People ppl = gson.fromJson(res.getJson().toStringUtf8(), People.class);

      // Print results
      System.out.printf("people found: %d\n", ppl.all.size());
      ppl.all.forEach(person -> System.out.println(person.name));
      System.out.println("Sleeping for 1 second");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  static class Person {
    String name;

    Person() {}
  }

  static class People {
    List<Person> all;

    People() {}
  }
}
