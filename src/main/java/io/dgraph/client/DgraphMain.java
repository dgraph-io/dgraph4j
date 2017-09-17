package io.dgraph.client;

import java.time.ZonedDateTime;

import io.dgraph.entity.DgraphRequest;
import io.dgraph.entity.Edge;
import io.dgraph.entity.Node;

public class DgraphMain {

    public static void main(final String[] args) {
        final DgraphClient dgraphClient = GrpcDgraphClient.newInstance("127.0.0.1", 9080);

        DgraphRequest req = new DgraphRequest();

        Node n = dgraphClient.NodeBlank("DummyName");
        System.out.println(n.getUid());
        Edge e = n.Edge("myDateKey");
        ZonedDateTime a = ZonedDateTime.now();
        e.SetValueDatetime(a);

        Edge e2 = n.Edge("myNameKey");
        e2.setValueString("asdsas");

        req.set(e);
        req.set(e2);
        DgraphResult resp = dgraphClient.query(req);

        final DgraphResult result2 = dgraphClient.query("{me(func:uid(" + n.getUid() + ")) { uid,myDateKey,myNameKey}}");

        System.out.println("uh" + result2.toJsonObject());
    }
}