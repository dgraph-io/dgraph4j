# Dgraph Client for Java

A minimal, basic implementation for a Dgraph client in Java using [grpc].

[grpc]: https://grpc.io/

This client following the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgraph/tree/master/client

## Quickstart

### Run latest Dgraph server
We will be releasing Dgraph v0.9 soon. Till then, the code in this repo will
work only with [Dgraph master][dgraph]. You will need Go installed and ensure
that `$GOPATH/bin` is added to your `$PATH`.

Execute the following commands in two different directories:

```
rm -r zw; go install github.com/dgraph-io/dgraph/dgraph && dgraph zero
```

```
rm -r p w; go install github.com/dgraph-io/dgraph/dgraph && dgraph server --memory_mb=1024
```

To test the client against the server, run:

```shell
$ ./gradlew test
```

### Using the client.

_More detailed instructions are coming soon_

Here is a snippet of code using the Dgraph client library

```java
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

public class DgraphMain {

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
    String name = json.getAsJsonArray("me").get(0).getAsJsonObject().get("name").getAsString();
  }
}
```

## Distribution

Currently, given that this is the first version, the distribution is done via a fatJar
built locally. The procedure to build it is:

```shell
$ ./gradlew fatJar
```

This will generate the client library (and it's dependencies) in a single jar at:
```shell
$ ls dgraph4j/build/libs
dgraph4j-all-0.0.1.jar
```

