package io.dgraph;

import static org.testng.Assert.assertEquals;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import org.testng.annotations.Test;

public class UpsertBlockTest extends DgraphIntegrationTest {
  @Test
  public void upsertBlockTest() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .")
            .build();
    dgraphClient.alter(op);

    String query =
        ""
            + "{\n"
            + "    me(func: eq(email, \"ashish@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";

    JsonArray jA = new JsonArray();
    JsonObject p1 = new JsonObject();
    p1.addProperty("uid", "uid(v)");
    p1.addProperty("name", "wrong");
    jA.add(p1);

    JsonObject p2 = new JsonObject();
    p2.addProperty("email", "ashish@dgraph.io");
    p2.addProperty("uid", "uid(v)");
    jA.add(p2);

    Mutation mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jA.toString())).build();
    Request req = Request.newBuilder().addMutations(mu).setQuery(query).build();

    Transaction txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    String query2 =
        ""
            + "{\n"
            + "    me(func: eq(email, \"ashish@dgraph.io\")) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";

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

    mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jA.toString())).build();
    req = Request.newBuilder().addMutations(mu).setQuery(query).build();

    txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    txn = dgraphClient.newTransaction();
    response = txn.query(query2);
    res = response.getJson().toStringUtf8();
    String exp2 = "{\"me\":[{\"name\":\"ashish\",\"email\":\"ashish@dgraph.io\"}]}";
    assertEquals(res, exp2);

    mu =
        Mutation.newBuilder()
            .setDelNquads(ByteString.copyFromUtf8("uid(v) <name> * .\nuid(v) <email> * ."))
            .setCond("@if(eq(len(v), 1))")
            .build();
    req = Request.newBuilder().addMutations(mu).setQuery(query).build();

    txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    txn = dgraphClient.newTransaction();
    response = txn.query(query2);
    res = response.getJson().toStringUtf8();
    String exp3 = "{\"me\":[]}";
    assertEquals(res, exp3);
  }

  @Test
  public void upsertBlockTestMultipleUID() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .\nname: string @index(exact) .")
            .build();
    dgraphClient.alter(op);

    JsonArray jA = new JsonArray();
    JsonObject p1 = new JsonObject();
    p1.addProperty("uid", "_:alice");
    p1.addProperty("name", "alice");
    jA.add(p1);

    JsonObject p2 = new JsonObject();
    p2.addProperty("email", "one@dgraph.io");
    p2.addProperty("uid", "_:alice");
    jA.add(p2);

    JsonObject p3 = new JsonObject();
    p3.addProperty("uid", "_:bob");
    p3.addProperty("name", "bob");
    jA.add(p3);

    JsonObject p4 = new JsonObject();
    p4.addProperty("email", "one@dgraph.io");
    p4.addProperty("uid", "_:bob");
    jA.add(p4);

    Mutation mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jA.toString())).build();
    Request req = Request.newBuilder().addMutations(mu).build();

    Transaction txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    String query =
        ""
            + "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";

    jA = new JsonArray();
    p1 = new JsonObject();
    p1.addProperty("uid", "uid(v)");
    p1.addProperty("email", "two@dgraph.io");
    jA.add(p1);

    mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jA.toString())).build();
    req = Request.newBuilder().addMutations(mu).setQuery(query).build();

    txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    String query2 =
        ""
            + "{\n"
            + "    me(func: eq(email, \"two@dgraph.io\")) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";

    txn = dgraphClient.newTransaction();
    Response response = txn.query(query2);
    String res = response.getJson().toStringUtf8();
    String exp =
        "{\"me\":[{\"name\":\"alice\",\"email\":\"two@dgraph.io\"},{\"name\":\"bob\",\"email\":\"two@dgraph.io\"}]}";
    assertEquals(res, exp);
  }

  @Test
  public void upsertBlockTestBulkDelete() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .\nname: string @index(exact) .")
            .build();
    dgraphClient.alter(op);

    JsonArray jA = new JsonArray();
    JsonObject p1 = new JsonObject();
    p1.addProperty("uid", "_:alice");
    p1.addProperty("name", "alice");
    jA.add(p1);

    JsonObject p2 = new JsonObject();
    p2.addProperty("email", "one@dgraph.io");
    p2.addProperty("uid", "_:alice");
    jA.add(p2);

    JsonObject p3 = new JsonObject();
    p3.addProperty("uid", "_:bob");
    p3.addProperty("name", "bob");
    jA.add(p3);

    JsonObject p4 = new JsonObject();
    p4.addProperty("email", "one@dgraph.io");
    p4.addProperty("uid", "_:bob");
    jA.add(p4);

    Mutation mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jA.toString())).build();
    Request req = Request.newBuilder().addMutations(mu).build();

    Transaction txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    String query =
        ""
            + "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\")) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";

    // Verify if records are successfully inserted.
    txn = dgraphClient.newTransaction();
    Response response = txn.query(query);
    String res = response.getJson().toStringUtf8();
    String exp =
        "{\"me\":[{\"name\":\"alice\",\"email\":\"one@dgraph.io\"},{\"name\":\"bob\",\"email\":\"one@dgraph.io\"}]}";
    assertEquals(res, exp);

    // DELETE using UPSERT.
    String query2 =
        ""
            + "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";

    jA = new JsonArray();
    p1 = new JsonObject();
    p1.addProperty("uid", "uid(v)");
    p1.addProperty("email", "one@dgraph.io");
    jA.add(p1);

    mu = Mutation.newBuilder().setDeleteJson(ByteString.copyFromUtf8(jA.toString())).build();
    req = Request.newBuilder().addMutations(mu).setQuery(query2).build();

    txn = dgraphClient.newTransaction();
    txn.doRequest(req);
    txn.commit();

    // GET again using email.
    txn = dgraphClient.newTransaction();
    response = txn.query(query);
    res = response.getJson().toStringUtf8();
    exp = "{\"me\":[]}";
    assertEquals(res, exp);
  }
}
