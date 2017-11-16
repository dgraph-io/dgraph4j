# Dgraph Client for Java

A minimal implementation for a Dgraph client in Java using [grpc].

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
- [Development](#development)
  * [Building the source](#building-the-source)
  * [Code Style](#code-style)
  * [Running unit tests](#running-unit-tests)

## Download
_TODO add a link to jar file_

grab via Maven:
```xml
<dependency>
  <groupId>io.dgraph</groupId>
  <artifactId>dgraph4j</artifactId>
  <version>0.9.2</version>
</dependency>
```
or Gradle:
```groovy
compile 'io.dgraph:dgraph4j:0.9.2'
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
DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
DgraphClient dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));
```

### Alter the database

To set the schema, create an `Operation` object, and pass it to 
`DgraphClient#alter` method.

```java
String schema = "name: string @index(term) .\n" +
                "balance: int .";

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

### Run a query

You can run a query by calling `txn.Query`. The response would contain a `JSON`
field, which has the JSON encoded result. You can unmarshal it into Go struct
via `json.Unmarshal`.

```go
	// Query the balance for Alice and Bob.
	const q = `
		{
			all(func: anyofterms(name, "Alice Bob")) {
				uid
				balance
			}
		}
	`
	resp, err := txn.Query(context.Background(), q)
	if err != nil {
		log.Fatal(err)
	}

	// After we get the balances, we have to decode them into structs so that
	// we can manipulate the data.
	var decode struct {
		All []struct {
			Uid     string
			Balance int
		}
	}
	if err := json.Unmarshal(resp.GetJson(), &decode); err != nil {
		log.Fatal(err)
	}
```

### Run a mutation

`txn.Mutate` would run the mutation. It takes in a `protos.Mutation` object,
which provides two main ways to set data: JSON and RDF N-Quad. You can choose
whichever way is convenient.

We're going to continue using JSON. You could modify the Go structs parsed from
the query, and marshal them back into JSON.

```go
	// Move $5 between the two accounts.
	decode.All[0].Bal += 5
	decode.All[1].Bal -= 5

	out, err := json.Marshal(decode.All)
	if err != nil {
		log.Fatal(err)
	}

	_, err := txn.Mutate(ctx, &protos.Mutation{SetJSON: out})
```

Sometimes, you only want to commit mutation, without querying anything further.
In such cases, you can use a `CommitNow` field in `protos.Mutation` to
indicate that the mutation must be immediately committed.

### Commit the transaction

Once all the queries and mutations are done, you can commit the transaction. It
returns an error in case the transaction could not be committed.

```go
	// Finally, we can commit the transactions. An error will be returned if
	// other transactions running concurrently modify the same data that was
	// modified in this transaction. It is up to the library user to retry
	// transactions when they fail.

	err := txn.Commit(ctx)
```

## Development

### Building the source

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
Make sure you have a Dgraph server running on localhost before you run this task.

```
./gradlew test
```

