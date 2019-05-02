# Dgraph Client for Java

[![Build Status](https://travis-ci.org/dgraph-io/dgraph4j.svg?branch=dj%2Ftravis)](https://travis-ci.org/dgraph-io/dgraph4j)
[![Coverage Status](https://coveralls.io/repos/github/dgraph-io/dgraph4j/badge.svg)](https://coveralls.io/github/dgraph-io/dgraph4j)

A minimal implementation for a Dgraph client for Java 1.8 and above, using [grpc].

[grpc]: https://grpc.io/

This client follows the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgo

Before using this client, we highly recommend that you go through [docs.dgraph.io],
and understand how to run and work with Dgraph.

[docs.dgraph.io]:https://docs.dgraph.io

## Table of Contents
- [Download](#download)
- [Quickstart](#quickstart)
- [Intro](#intro)
- [Using the Synchronous Client](#using-the-synchronous-client)
  * [Create the client](#create-the-client)
  * [Alter the database](#alter-the-database)
  * [Create a transaction](#create-a-transaction)
  * [Run a mutation](#run-a-mutation)
  * [Run a query](#run-a-query)
  * [Commit a transaction](#commit-a-transaction)
  * [Setting Deadlines](#setting-deadlines)
  * [Setting Metadata Headers](#setting-metadata-headers)
  * [Helper Methods](#helper-methods)
* [Using the Asynchronous Client](#using-the-asynchronous-client)
* [Checking the request latency](#checking-the-request-latency)
- [Development](#development)
  * [Building the source](#building-the-source)
  * [Code Style](#code-style)
  * [Running unit tests](#running-unit-tests)

## Download
grab via Maven:
```xml
<dependency>
  <groupId>io.dgraph</groupId>
  <artifactId>dgraph4j</artifactId>
  <version>1.7.3</version>
</dependency>
```
or Gradle:
```groovy
compile 'io.dgraph:dgraph4j:1.7.3'
```

## Quickstart
Build and run the [DgraphJavaSample] project in the `samples` folder, which
contains an end-to-end example of using the Dgraph Java client. Follow the
instructions in the README of that project.

[DgraphJavaSample]: https://github.com/dgraph-io/dgraph4j/tree/master/samples/DgraphJavaSample

## Intro
This library supports two styles of clients, the synchronous client `DgraphClient` and
the async client `DgraphAsyncClient`.
A `DgraphClient` or `DgraphAsyncClient` can be initialised by passing it
a list of `DgraphBlockingStub` clients. The `anyClient()` API can randomly pick a stub, which can
then be used for GRPC operations. In the next section, we will explain how to create a
synchronous client and use it to mutate or query dgraph. For the async client, more details can
be found in the [Using the Asynchronous Client](#using-the-asynchronous-client) section.

## Using the Synchronous Client

### Create the client

The following code snippet shows how to create a synchronous client using just one connection.

```java
ManagedChannel channel =
ManagedChannelBuilder.forAddress("localhost", 9080).usePlaintext(true).build();
DgraphStub stub = DgraphGrpc.newStub(channel);
DgraphClient dgraphClient = new DgraphClient(Collections.singletonList(stub));
```

Alternatively, you can specify a deadline (in seconds) after which the client will time out when
making requests to the server.

```java
DgraphClient dgraphClient = new DgraphClient(stub);
```

### Alter the database

To set the schema, create an `Operation` object, set the schema and pass it to
`DgraphClient#alter` method.

```java
String schema = "name: string @index(exact) .";
Operation op = Operation.newBuilder().setSchema(schema).build();
dgraphClient.alter(op);
```

`Operation` contains other fields as well, including drop predicate and
drop all. Drop all is useful if you wish to discard all the data, and start from
a clean slate, without bringing the instance down.

```java
// Drop all data including schema from the dgraph instance. This is useful
// for small examples such as this, since it puts dgraph into a clean
// state.
dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
```

### Create a transaction

There are two types of transactions in dgraph, i.e. the read-only transactions that only include
queries and the transactions that change data in dgraph with mutate operations. Both the
synchronous client `DgraphClient` and the async client `DgraphAsyncClient` support the two types
of transactions by providing the `newTransaction` and the `newReadOnlyTransaction` APIs. Creating
 a transaction is a local operation and incurs no network overhead.

In most of the cases, the normal read-write transactions should be used, which can have any
number of query, or mutate operations. However, if a transaction only has queries, you might
benefit from a read-only transaction, which can share the same read timestamp across multiple
such read-only transactions, thus, potentially providing better latency.

For normal read-write transactions, it is a good practise to call `Transaction#discard()` in a
`finally` block after running the transaction. Calling `Transaction#discard()` after
`Transaction#commit()` is a no-op and you can call `discard()` multiple times with no additional
side-effects.

```java
Transaction txn = dgraphClient.newTransaction();
  try {
    // Do something here
    // ...
  } finally {
    txn.discard();
  }
```
For read-only transactions, there is no need to call `Transaction.discard`, which is equivalent
to a no-op.

### Run a mutation
`Transaction#mutate` runs a mutation. It takes in a `Mutation` object,
which provides two main ways to set data: JSON and RDF N-Quad. You can choose
whichever way is convenient.

We're going to use JSON. First we define a `Person` class to represent a person.
This data will be seralized into JSON.

```java
class Person {
        String name
        Person() {}
}
```

Next, we initialise a `Person` object, serialize it and use it in `Mutation` object.

```java
// Create data
Person p = new Person();
p.name = "Alice";

// Serialize it
Gson gson = new Gson();
String json = gson.toJson(p);
// Run mutation
Mutation mu =
  Mutation.newBuilder()
  .setSetJson(ByteString.copyFromUtf8(json.toString()))
  .build();

txn.mutate(mu);
```

Sometimes, you only want to commit mutation, without querying anything further.
In such cases, you can use a `CommitNow` field in `Mutation` object to
indicate that the mutation must be immediately committed.

The `IgnoreIndexConflict` flag can be set to `true` on the `Mutation` object
to not run conflict detection over the index, which would decrease the number
of transaction conflicts and aborts. However, this would come at the cost of
potentially inconsistent upsert operations.

### Commit a transaction
A transaction can be committed using the `Transaction#commit()` method. If your transaction
consisted solely of calls to `Transaction#query()`, and no calls to `Transaction#mutate()`,
then calling `Transaction#commit()` is not necessary.

An error will be returned if other transactions running concurrently modify the same data that was
modified in this transaction. It is up to the user to retry transactions when they fail.

```java
Transaction txn = dgraphClient.newTransaction();

try {
  // …
  // Perform any number of queries and mutations
  //…
  // and finally…
  txn.commit()
} catch (TxnConflictException ex) {
   // Retry or handle exception.
} finally {
   // Clean up. Calling this after txn.commit() is a no-op
   // and hence safe.
   txn.discard();
}
```

### Run a query
You can run a query by calling `Transaction#query()`. You will need to pass in a GraphQL+-
query string, and a map (optional, could be empty) of any variables that you might want to
set in the query.

The response would contain a `JSON` field, which has the JSON encoded result. You will need
to decode it before you can do anything useful with it.

Let’s run the following query:

```
query all($a: string) {
  all(func: eq(name, $a)) {
            name
  }
}
```

First we must create a `People` class that will help us deserialize the JSON result:

```java
class People {
  List<Person> all;
  People() {}
}
```

Then we run the query, deserialize the result and print it out:

```java
// Query
String query =
"query all($a: string){\n" +
"  all(func: eq(name, $a)) {\n" +
"    name\n" +
"  }\n" +
"}\n";

Map<String, String> vars = Collections.singletonMap("$a", "Alice");
Response res = dgraphClient.newReadOnlyTransaction().queryWithVars(query, vars);

// Deserialize
People ppl = gson.fromJson(res.getJson().toStringUtf8(), People.class);

// Print results
System.out.printf("people found: %d\n", ppl.all.size());
ppl.all.forEach(person -> System.out.println(person.name));
```
This should print:

```
people found: 1
Alice
```


### Setting deadlines
It is recommended that you always set a deadline for each client call, after
which the client terminaltes. This is
in line with the recommendation for any gRPC client. Read [this forum post][deadline-post]
for more details.

```java
channel = ManagedChannelBuilder.forAddress("localhost", 9080).usePlaintext(true).build();
DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
ClientInterceptor timeoutInterceptor = new ClientInterceptor(){
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return next.newCall(method, callOptions.withDeadlineAfter(500, TimeUnit.MILLISECONDS));
  }
};
stub.withInterceptors(timeoutInterceptor);
DgraphClient dgraphClient = new DgraphClient(stub);
```

[deadline-post]: https://discuss.dgraph.io/t/dgraph-java-client-setting-deadlines-per-call/3056

### Setting Metadata Headers
Certain headers such as authentication tokens need to be set globally for all subsequent calls.
Below is an example of setting a header with the name "auth-token":
```java
// create the stub first
ManagedChannel channel =
ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build();
DgraphStub stub = DgraphGrpc.newStub(channel);

// use MetadataUtils to augment the stub with headers
Metadata metadata = new Metadata();
metadata.put(
  Metadata.Key.of("auth-token", Metadata.ASCII_STRING_MARSHALLER), "the-auth-token-value");
stub = MetadataUtils.attachHeaders(stub, metadata);

// create the DgraphClient wrapper around the stub
DgraphClient dgraphClient = new DgraphClient(stub);

// trigger a RPC call using the DgraphClient
dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
```
### Helper Methods

#### Delete multiple edges
The example below uses the helper method `Helpers#deleteEdges` to delete
multiple edges corresponding to predicates on a node with the given uid.
The helper method takes an existing mutation, and returns a new mutation
with the deletions applied.

```java
 Mutation mu = Mutation.newBuilder().build()
 mu = Helpers.deleteEdges(mu, uid, "friends", "loc");
 dgraphClient.newTransaction().mutate(mu);
```

## Using the Asynchronous Client
Dgraph Client for Java also bundles an asynchronous API, which can be used by
instantiating the `DgraphAsyncClient` class. The usage is almost exactly the
same as the `DgraphClient` (show in previous section) class. The main
differences is that the `DgraphAsyncClient#newTransacation()` returns an
`AsyncTransaction` class. The API for `AsyncTransaction` is exactly
`Transcation`. The only difference is that instead of returning the results
directly, it returns immediately with a corresponding `CompletableFuture<T>`
object. This object represents the computation which runs asynchronously to
yield the result in the future. Read more about `CompletableFuture<T>` in the
[Java 8 documentation][futuredocs].

[futuredocs]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html

Here is the asynchronous version of the code above, which runs a query.

```java
// Query
String query =
"query all($a: string){\n" +
"  all(func: eq(name, $a)) {\n" +
"    name\n" +
  }\n" +
"}\n";

Map<String, String> vars = Collections.singletonMap("$a", "Alice");

AsyncTransaction txn = dgraphAsyncClient.newTransaction();
txn.query(query).thenAccept(response -> {
    // Deserialize
    People ppl = gson.fromJson(res.getJson().toStringUtf8(), People.class);

    // Print results
    System.out.printf("people found: %d\n", ppl.all.size());
    ppl.all.forEach(person -> System.out.println(person.name));
});
```
## Checking the request latency
If you would like to see the latency for either a mutation or
query request, the latency field in the returned result can be helpful. Here is an example to log
 the latency of a query request:
```java
      Response resp = txn.query(query);
      Latency latency = resp.getLatency();
      logger.info("parsing latency:" + latency.getParsingNs());
      logger.info("processing latency:" + latency.getProcessingNs());
      logger.info("encoding latency:" + latency.getEncodingNs());
```
Similarly you can get the latency of a mutation request:
```java
    Assigned assignedIds = dgraphClient.newTransaction().mutate(mu);
    Latency latency = assignedIds.getLatency();
```

## Development

### Building the source

**Warning**: The gradle build runs integration tests on a locally running Dgraph server.
The tests will remove all data from your Dgraph instance. So make sure that you don't
have any important data on your Dgraph instance.
```
./gradlew build
```
If you have made changes to the `task.proto` file, this step will also regenerate the source files
generated by Protocol Buffer tools.

### Code Style
We use [google-java-format] to format the source code. If you run `./gradlew build`, you will be
warned if there is code that is not conformant. You can run `./gradlew goJF` to format the source
 code, before committing it.

[google-java-format]:https://github.com/google/google-java-format

### Running unit tests
**Warning**: This command will runs integration tests on a locally running Dgraph server.
The tests will remove all data from your Dgraph instance. So make sure that you don't
have any important data on your Dgraph instance.

Make sure you have a Dgraph server running on localhost before you run this task.

```
./gradlew test
```
