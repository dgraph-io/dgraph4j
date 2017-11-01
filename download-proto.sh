#!/bin/sh

cd "`dirname $0`"

echo "Downloading graphresponse.proto..."
curl -s -o /tmp/graphresponse.proto "https://raw.githubusercontent.com/dgraph-io/dgraph/master/protos/graphresponse.proto"
LINE1=`grep -n "^package" /tmp/graphresponse.proto | cut -d':' -f1`
LINE2=`echo "$LINE1+1" | bc`
head -$LINE1 /tmp/graphresponse.proto > src/main/proto/graphresponse.proto
cat << EOF >> src/main/proto/graphresponse.proto
option java_multiple_files = true;
option java_package = "io.dgraph.proto";
option java_outer_classname = "GraphResponse";
EOF
tail +$LINE2 /tmp/graphresponse.proto >> src/main/proto/graphresponse.proto


echo "Downloading facets.proto..."
curl -s -o /tmp/facets.proto "https://raw.githubusercontent.com/dgraph-io/dgraph/master/protos/facets.proto"
LINE1=`grep -n "^package" /tmp/facets.proto | cut -d':' -f1`
LINE2=`echo "$LINE1+1" | bc`
head -$LINE1 /tmp/facets.proto > src/main/proto/facets.proto
cat << EOF >> src/main/proto/facets.proto
option java_multiple_files = true;
option java_package = "io.dgraph.proto.facets";
option java_outer_classname = "FacetsProto";
EOF
tail +$LINE2 /tmp/facets.proto >> src/main/proto/facets.proto

echo "Downloading schema.proto..."
curl -s -o src/main/proto/schema.proto "https://raw.githubusercontent.com/dgraph-io/dgraph/master/protos/schema.proto"

echo "Done."
echo "Run './gradlew fatJar' to build."