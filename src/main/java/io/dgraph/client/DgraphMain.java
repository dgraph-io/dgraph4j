package io.dgraph.client;

import java.time.ZonedDateTime;

import io.dgraph.entity.DgraphRequest;
import io.dgraph.entity.Edge;
import io.dgraph.entity.Node;

public class DgraphMain {

    public static void main(final String[] args) {
        final DgraphClient dgraphClient = GrpcDgraphClient.newInstance("127.0.0.1", 9080);

        DgraphRequest req = new DgraphRequest();

        Node n = dgraphClient.NodeBlank("2132ll");
        System.out.println(n.getUid());
        Edge e = n.Edge("date22");
        ZonedDateTime a = ZonedDateTime.now();
        e.SetValueDatetime(a);

        Edge e2 = n.Edge("zzzz0");
        e2.setValueString("asdsas");

        req.set(e);
        req.set(e2);
        DgraphResult resp = dgraphClient.query(req);

        System.out.println("{me(func:uid(" + n.getUid() + ")) { uid,date22,na8}}");
        final DgraphResult result2 = dgraphClient.query("{me(func:uid(" + n.getUid() + ")) { uid,date22,zzzz0}}");

        // final DgraphResult result = dgraphClient.query("{me(uid: 0xa}");
        System.out.println("uh" + result2.toJsonObject());
    }
}