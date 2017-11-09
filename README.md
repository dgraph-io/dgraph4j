# Dgraph Client for Java

This is a very basic implementation for a Dgraph client in Java using Grpc.

To test the client code, you need a running instance of Dgraph in the following location:
* host: `localhost`
* port: `8081`

To run the tests simply execute:
```shell
$ ./gradlew test
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

**NOTE**: there's no discussion yet on how to (or if) distribute this package in any of the JAR
  repositories publicly available, e.g. Maven Central.

## How to use it?

You just need to include the fatJar into the classpath, the following is a simple
example of how to use it:

* Write `DgraphMain.java` (assuming Dgraph contains the data required for the query):
```java
import DgraphClient;
import DgraphClient;
import io.dgraph.client.DgraphResult;

public class DgraphMain {

    public static void main(final String[] args) {
        final DgraphClient dgraphClient = GrpcDgraphClient.newInstance("localhost", 8081);
        final DgraphResult result = dgraphClient.query("{me(_xid_: alice) { name _xid_ follows { name _xid_ follows {name _xid_ } } }}");
        System.out.println(result.toJsonObject().toString());
    }
}
```

* Compile:
```shell
$ javac -cp dgraph4j-all-0.0.1.jar DgraphMain.java
```

* Run:
```shell
$ java -cp dgraph4j-all-0.0.1.jar:. DgraphMain
Jun 29, 2016 12:28:03 AM io.grpc.internal.ManagedChannelImpl <init>
INFO: [ManagedChannelImpl@5d3411d] Created with target localhost:8081
{"_root_":[{"_uid_":"0x8c84811dffd0a905","_xid_":"alice","name":"Alice","follows":[{"_uid_":"0xdd77c65008e3c71","_xid_":"bob","name":"Bob"},{"_uid_":"0x5991e7d8205041b3","_xid_":"greg","name":"Greg"}]}],"server_latency":{"pb":"11.487µs","parsing":"85.504µs","processing":"270.597µs"}}
```

## Word of caution

**This client is being run and maintained not by Dgraph team, but by the larger Dgraph community.** As such it might lag behind the latest release. We readily welcome PRs for this client.
