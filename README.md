# Dgraph Client for Java

[![Build Status](https://travis-ci.org/dgraph-io/dgraph4j.svg?branch=dj%2Ftravis)](https://travis-ci.org/dgraph-io/dgraph4j)
[![Coverage Status](https://coveralls.io/repos/github/dgraph-io/dgraph4j/badge.svg)](https://coveralls.io/github/dgraph-io/dgraph4j)

A minimal implementation for a Dgraph client for Java 1.8 and above, using [grpc].

[grpc]: https://grpc.io/

This client follows the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgraph/tree/master/client

Before using this client, we highly recommend that you go through [docs.dgraph.io],
and understand how to run and work with Dgraph.

[docs.dgraph.io]:https://docs.dgraph.io

## Table of Contents
- [Download](#download)
- [Quickstart](#quickstart)
- [Using the Client](#using-the-client)
  * [Create the client](#create-the-client)
  * [Alter the database](#alter-the-database)
  * [Create a transaction](#create-a-transaction)
  * [Run a mutation](#run-a-mutation)
  * [Run a query](#run-a-query)
  * [Commit a transaction](#commit-a-transaction)
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
  <version>1.3.0</version>
</dependency>
```
or Gradle:
```groovy
compile 'io.dgraph:dgraph4j:1.3.0'
```

## Quickstart
Build and run the [DgraphJavaSample] project in the `samples` folder, which
contains an end-to-end example of using the Dgraph Java client. Follow the
instructions in the README of that project.

[DgraphJavaSample]: https://github.com/dgraph-io/dgraph4j/tree/master/samples/DgraphJavaSample

## Using the Client

### Create the client
a `DgraphClient` object can be initialised by passing it a list of `DgraphBlockingStub`
clients. Connecting to multiple Dgraph servers in the same cluster allows for better
distribution of workload.

The following code snippet shows just one connection.

```java
ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9080).usePlaintext(true).build();
DgraphClientPool pool = new DgraphClientPool(Collections.singletonList(channel));
DgraphClient dgraphClient = new DgraphClient(pool);
```

Alternatively, you can specify a deadline (in seconds) after which the client will time out when making
requests to the server.

```java
DgraphClientPool pool = new DgraphClientPool(Collections.singletonList(channel), 60); // 1 min timeout
DgraphClient dgraphClient = new DgraphClient(pool);
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

To create a transaction, call `DgraphClient#newTransaction()` method, which returns a
new `Transaction` object. This operation incurs no network overhead.

It is good practise to call `Transaction#discard()` in a `finally` block after running
the transaction. Calling `Transaction#discard()` after `Transaction#commit()` is a no-op
and you can call `discard()` multiple times with no additional side-effects.

```java
Transaction txn = dgraphClient.newTransaction();
  try {
    // Do something here
    // ...
  } finally {
    txn.discard();
  }
```

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
Response res = dgraphClient.newTransaction().queryWithVars(query, vars);

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
We use [google-java-format] to format the source code. If you run `./gradlew build`, you will be warned
if there is code that is not conformant. You can run `./gradlew goJF` to format the source code, before
commmitting it.

[google-java-format]:https://github.com/google/google-java-format

### Running unit tests
**Warning**: This command will runs integration tests on a locally running Dgraph server. 
The tests will remove all data from your Dgraph instance. So make sure that you don't 
have any important data on your Dgraph instance.

Make sure you have a Dgraph server running on localhost before you run this task.

```
./gradlew test
```
