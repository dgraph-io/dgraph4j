Sample project demonstrating the use of [dgraph4j], the official Java client
for Dgraph.

[dgraph4j]: https://github.com/dgraph-io/dgraph4

## Running

### Start Dgraph Server

You will need to install [Dgraph v21.3.0 or above][releases] and start a local cluster as shown below.

[releases]: https://github.com/dgraph-io/dgraph/releases

First, create two separate directories for `dgraph zero` and `dgraph server`.

```
mkdir -p dgraphdata/zero dgraphdata/data
```

Then start `dgraph zero`:

```
cd dgraphdata/zero
rm -rf zw; dgraph zero
```

Finally, start the `dgraph alpha` server:

```
cd dgraphdata/data
rm -rf p w t; dgraph alpha --zero localhost:5080 -o 100
```

Notice that in the command above, we shifted the ports by 100 from the default ports of 7080 for
internal traffic, 8080 for http, and 9080 for GRPC, which means the alpha server is binding to
the port 7180 for internal traffic, 8180 for http, and 9180 for GRPC.

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

## Run the sample code

**Warning**: The sample code, when run, will remove all data from your locally running Dgraph instance.
So make sure that you don't have any important data on your Dgraph instance.

This example in [App.java:39](./src/main/java/App.java#L39-L47) creates the
DgraphStub with a deadline using a call interceptor to set timeouts **per
request**. This is most likely what you want to do. For more info, see [Setting
Deadlines](https://github.com/dgraph-io/dgraph4j/#setting-deadlines).

```java
stub =
    stub.withInterceptors(
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return next.newCall(method, callOptions.withDeadlineAfter(5, TimeUnit.SECONDS));
          }
        });
```

The following output is shown when running the example (`./gradlew run`):

```text
$ ./gradlew run
Loop iteration: 1
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 2
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 3
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 4
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 5
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 6
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 7
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 8
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 9
people found: 1
Alice
Sleeping for 1 second
Done!
Loop iteration: 10
people found: 1
Alice
Sleeping for 1 second
Done!
```

If you see the output `Done!`, then the example works as expected.
You can explore the source code in `src/main/java/App.java` file.
