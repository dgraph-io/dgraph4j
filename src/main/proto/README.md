# Development

For updates to the service, drop the [graphrespose.proto](https://github.com/dgraph-io/dgraph/blob/master/query/graph/graphresponse.proto) file here and add:

```
...
syntax="proto3";
package graph;

// Add java package here
option java_package = "io.dgraph.client";

service Dgraph {
    rpc Query (Request) returns (Response) {}
}
...
```
Once the java package has been specified, execute:
```
./gradlew build
```
in the root of the project to regenerate the service Java implementations of the Grpc calls.
