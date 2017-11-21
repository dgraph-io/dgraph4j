Sample project demonstrating the use of [dgraph4j], the official Java client
for Dgraph.

[dgraph4j]:https://github.com/dgraph-io/dgraph4

## Running
### Start Dgraph Server
You will need to install [Dgraph v0.9.2 or above][releases] and run it.

[releases]: https://github.com/dgraph-io/dgraph/releases


You can run the commands below to start a clean dgraph server everytime, for testing
and exploration.

First, create two separate directories for `dgraph zero` and `dgraph server`.

```
mkdir -p dgraphdata/zero dgraphdata/data
```

Then start `dgraph zero`:

```
cd dgraphdata/zero
rm -r zw; dgraph zero --port_offset -2000
```

Finally, start the `dgraph server`:

```
cd dgraphdata/data
rm -r p w; dgraph server --memory_mb=1024 --zero localhost:5080
```

For more configuration options, and other details, refer to [docs.dgraph.io](https://docs.dgraph.io)

## Run the sample code
```
$ ./gradlew run

> Task :run
people found: 1
Alice


BUILD SUCCESSFUL in 1s
2 actionable tasks: 2 executed

```

If you see `Alice` in the output, everything is working fine. You can explore the source code in `src/main/java/App.java` file.
