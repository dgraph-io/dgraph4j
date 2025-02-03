Sample project demonstrating the use of [dgraph4j], the official Java client for Dgraph.

[dgraph4j]: https://github.com/hypermodeinc/dgraph4

## Running

### Start Dgraph Server

You will need to install [Dgraph v1.1.0 or above][releases] and run it.

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
rm -rf p w; dgraph alpha --zero localhost:5080 -o 100
```

Notice that in the command above, we shifted the ports by 100 from the default ports of 7080 for
internal traffic, 8080 for http, and 9080 for GRPC, which means the alpha server is binding to the
port 7180 for internal traffic, 8180 for http, and 9180 for GRPC.

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

## Run the sample code

**Warning**: The sample code, when run, will remove all data from your locally running Dgraph
instance. So make sure that you don't have any important data on your Dgraph instance.

```
$ ./gradlew run

> Task :run
people found: 1
Alice


BUILD SUCCESSFUL in 1s
2 actionable tasks: 2 executed

```

If you see `Alice` in the output, everything is working fine. You can explore the source code in
`src/main/java/App.java` file.
