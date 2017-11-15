# Dgraph Client for Java

A minimal, basic implementation for a Dgraph client in Java using [grpc].

[grpc]: https://grpc.io/

This client follows the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgraph/tree/master/client

Before using this client, it is highly recommended that you go through [docs.dgraph.io],
and make sure you understand what Dgraph is all about, and how to run it.

[docs.dgraph.io]:https://docs.dgraph.io

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

For more configuration options, and other details, refer to [docs.dgraph.io]

### Using the Java client
This section will guide you in creating a Java project from scratch, and using the Dgraph Java
client to communicate with the  server. We will be using [gradle] as our build tool, so make
sure you have it installed, if you want to follow along.

[dgraph-io/DgraphJavaSample]:https://github.com/dgraph-io/DgraphJavaSample
[gradle]: https://gradle.org/

_For your convenience, all the files created and modified below can be found in the
[dgraph-io/DgraphJavaSample] repo._

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

// Use maven to pull down dependencies
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

Modify `src/main/java/App.java`, as follows. The program below makes a simple mutation, issues a query and
parses the JSON result returned by the server.

```java
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
    System.out.println(name);
  }
}
```

Modify `src/test/java/AppTest.java` as follows:

```java
import org.junit.Test;
import static org.junit.Assert.*;

public class AppTest {
    @Test public void testAppExists() {
        App classUnderTest = new App();
    }
}
```

Finally, run the program:

```shell
$ ./gradlew run

> Task :run 
Alice


BUILD SUCCESSFUL in 1s
2 actionable tasks: 1 executed, 1 up-to-date

```

If you see `Alice` in the output, you have a running client.

## Client API
_TODO_
### alter()
### newTransaction()
### Transaction::query()
### Transaction::mutate()
### Transaction::commit()
### Transaction::discard()

## Development

### Building the source

```shell
./gradle build
```
If you have made changes to the `task.proto` file, this step will also regenerate the source files
generated by Protocol Buffer tools.

### Code Style
We use [google-java-format] to format the source code. If you run `./gradlew build`, you will be warned
if there is code that is not conformant. You can run `./gradlew goJF` to format the source code, before
commmitting it.

[google-java-format]:https://github.com/google/google-java-format

### Testing the source
Make sure you have a Dgraph server running on localhost before you run this task.

```shell
./gradlew test
```

