Sample project demonstrating the use of [dgraph4j], the official Java client for Dgraph.

[dgraph4j]: https://github.com/hypermodeinc/dgraph4

## Running

### Start Dgraph Server

You will need to install [Dgraph v21.03.0 or above][releases] and run it.

[releases]: https://github.com/hypermodeinc/dgraph/releases

You can run the commands below to start a clean dgraph server everytime, for testing and
exploration.

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
internal traffic, 8080 for http, and 9080 for GRPC, which means the alpha server is binding to the
port 7180 for internal traffic, 8180 for http, and 9180 for GRPC.

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

## Run the sample code

**Warning**: The sample code, when run, will remove all data from your locally running Dgraph
instance. So make sure that you don't have any important data on your Dgraph instance.

This example in [App.java:34](./src/main/java/App.java#L34) creates the DgraphStub with a deadline
set for the **entire life of the stub**. This is most likely what you do NOT want to do. For more
info, see [Setting Deadlines](https://github.com/hypermodeinc/dgraph4j/#setting-deadlines).

```java
stub = stub.withDeadlineAfter(5, TimeUnit.SECONDS);
```

```text
$ ./gradlew run
Loop iteration: 1
people found: 1
Alice
Sleeping for 1 second
Loop iteration: 2
people found: 1
Alice
Sleeping for 1 second
Loop iteration: 3
people found: 1
Alice
Sleeping for 1 second
Loop iteration: 4
people found: 1
Alice
Sleeping for 1 second
Loop iteration: 5
people found: 1
Alice
Sleeping for 1 second
Loop iteration: 6
Exception in thread "main" java.lang.RuntimeException: java.util.concurrent.CompletionException: java.lang.RuntimeException: The doRequest encountered an execution exception:
        at io.dgraph.AsyncTransaction.lambda$doRequest$2(AsyncTransaction.java:226)
        at java.util.concurrent.CompletableFuture.uniHandle(CompletableFuture.java:836)
        at java.util.concurrent.CompletableFuture$UniHandle.tryFire(CompletableFuture.java:811)
        at java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:488)
        at java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1609)
        at java.util.concurrent.CompletableFuture$AsyncSupply.exec(CompletableFuture.java:1596)
        at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
        at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
        at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
        at java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:175)
Caused by: java.util.concurrent.CompletionException: java.lang.RuntimeException: The doRequest encountered an execution exception:
        at java.util.concurrent.CompletableFuture.encodeThrowable(CompletableFuture.java:273)
        at java.util.concurrent.CompletableFuture.completeThrowable(CompletableFuture.java:280)
        at java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1606)
        ... 5 more
Caused by: java.lang.RuntimeException: The doRequest encountered an execution exception:
        at io.dgraph.DgraphAsyncClient.lambda$runWithRetries$2(DgraphAsyncClient.java:248)
        at java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1604)
        ... 5 more
Caused by: java.util.concurrent.ExecutionException: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: ClientCall started after deadline exceeded: -0.923822565s from now
        at java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:357)
        at java.util.concurrent.CompletableFuture.get(CompletableFuture.java:1908)
        at io.dgraph.DgraphAsyncClient.lambda$runWithRetries$2(DgraphAsyncClient.java:216)
        ... 6 more
Caused by: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: ClientCall started after deadline exceeded: -0.923822565s from now
        at io.grpc.Status.asRuntimeException(Status.java:533)
        at io.grpc.stub.ClientCalls$StreamObserverToCallListenerAdapter.onClose(ClientCalls.java:478)
        at io.grpc.internal.ClientCallImpl.closeObserver(ClientCallImpl.java:617)
        at io.grpc.internal.ClientCallImpl.access$300(ClientCallImpl.java:70)
        at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInternal(ClientCallImpl.java:803)
        at io.grpc.internal.ClientCallImpl$ClientStreamListenerImpl$1StreamClosed.runInContext(ClientCallImpl.java:782)
        at io.grpc.internal.ContextRunnable.run(ContextRunnable.java:37)
        at io.grpc.internal.SerializingExecutor.run(SerializingExecutor.java:123)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
```

If you see the output `ClientCall started after deadline exceeded`, then the example works as
expected. You can explore the source code in `src/main/java/App.java` file.
