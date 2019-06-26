package io.dgraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;
import io.dgraph.DgraphProto.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UpsertBlockTest extends DgraphIntegrationTest {
    @Test
    public void upsertBlockTest() throws Exception {
        DgraphProto.Operation op =
                DgraphProto.Operation.newBuilder()
                        .setSchema("email: string @index(exact) @upsert .").build();
        dgraphClient.alter(op);

        String query = "" +
                "{\n" +
                "    me(func: eq(email, \"ashish@dgraph.io\")) {\n" +
                "        v as uid\n" +
                "    }\n" +
                "}\n";

        JsonArray jA = new JsonArray();
        JsonObject p1 = new JsonObject();
        p1.addProperty("uid", "uid(v)");
        p1.addProperty("name", "wrong");
        jA.add(p1);

        JsonObject p2 = new JsonObject();
        p2.addProperty("email", "ashish@dgraph.io");
        p2.addProperty("uid", "uid(v)");
        jA.add(p2);

        Mutation mu = Mutation.newBuilder()
                .setQuery(query)
                .setSetJson(ByteString.copyFromUtf8(jA.toString()))
                .build();

        Transaction txn = dgraphClient.newTransaction();
        txn.mutate(mu);
        txn.commit();

        String query2 = "" +
                "{\n" +
                "    me(func: eq(email, \"ashish@dgraph.io\")) {\n" +
                "        name\n" +
                "        email\n" +
                "    }\n" +
                "}\n";

        txn = dgraphClient.newTransaction();
        Response response = txn.query(query2);
        String res = response.getJson().toStringUtf8();
        String exp1 = "{\"me\":[{\"name\":\"wrong\",\"email\":\"ashish@dgraph.io\"}]}";
        assertEquals(res, exp1);

        jA = new JsonArray();
        p1 = new JsonObject();
        p1.addProperty("uid", "uid(v)");
        p1.addProperty("name", "ashish");
        jA.add(p1);

        mu = Mutation.newBuilder()
                .setQuery(query)
                .setSetJson(ByteString.copyFromUtf8(jA.toString()))
                .build();

        txn = dgraphClient.newTransaction();
        txn.mutate(mu);
        txn.commit();

        txn = dgraphClient.newTransaction();
        response = txn.query(query2);
        res = response.getJson().toStringUtf8();
        String exp2 = "{\"me\":[{\"name\":\"ashish\",\"email\":\"ashish@dgraph.io\"}]}";
        assertEquals(res, exp2);

        mu = Mutation.newBuilder()
                .setQuery(query)
                .setDelNquads(ByteString.copyFromUtf8("uid(v) <name> * .\nuid(v) <email> * ."))
                .build();

        txn = dgraphClient.newTransaction();
        txn.mutate(mu);
        txn.commit();

        txn = dgraphClient.newTransaction();
        response = txn.query(query2);
        res = response.getJson().toStringUtf8();
        String exp3 = "{\"me\":[]}";
        assertEquals(res, exp3);
    }
}
