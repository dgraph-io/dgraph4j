# Dgraph Client for Java

A minimal, basic implementation for a Dgraph client in Java using [grpc].

[grpc]: https://grpc.io/

This client following the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgraph/tree/master/client

## Quickstart

### Start Dgraph Server
You will need to install [Dgraph v0.9][releases] and run it. After installing
the server, running the following commands:

[releases]: https://github.com/dgraph-io/dgraph/releases

First, create two separate directories for `dgraph zero` and `dgraph server`.

```
mkdir -p dgraphdata/zero dgraphdata/data
```

Then start `dgraph zero`:

```
cd dgraphdata/zero
rm -r zw; dgraph zero
```

Finally, start the `dgraph server`:

```
cd dgraphdata/data
rm -r p w; dgraph server --memory_mb=1024
```

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

### Using the Java client
This section will guide you in creating a Java project from scratch, and using the Dgraph Java
client to communicate with the  server. We will be using [gradle] as our build tool, so make
you have it installed.

[gradle]: https://gradle.org/

First initialize a new `java-application` project using gradle.

```
mkdir DgraphJavaSample
cd DgraphJavaSample
gradle init --type java-application
```

Modify the `build.gradle` file to change the `repositories` and `dependencies`:

```groovy

// Apply the java plugin to add support for Java
apply plugin: 'java'

// Apply the maven plugin to add support for Maven
apply plugin: 'maven'

// Apply the application plugin to add support for building an application
apply plugin: 'application'

// In this section you declare where to find the dependencies of your project
repositories {
    mavenCentral()
}

dependencies {
 	// Use Dgraph Java client
 	compile 'io.dgraph:dgraph4j:0.9.1'

    // Use JUnit test framework
    testCompile 'junit:junit:4.12'
}

// Define the main class for the application
mainClassName = 'App'
```

Modify the class

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

## Development

### Building the source

### Testing the source
To test the client in this repo against the server, run:

```shell
$ ./gradlew test
```

