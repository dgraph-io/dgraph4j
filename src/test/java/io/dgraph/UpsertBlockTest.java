package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class UpsertBlockTest extends DgraphIntegrationTest {
  @Test
  public void upsertBlockTest() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .")
            .build();
    dgraphClient.alter(op);

    JsonArray jsonData1 = new JsonArray();
    JsonObject person1 = new JsonObject();
    person1.addProperty("uid", "uid(v)");
    person1.addProperty("name", "wrong");
    jsonData1.add(person1);

    JsonObject person2 = new JsonObject();
    person2.addProperty("email", "ashish@dgraph.io");
    person2.addProperty("uid", "uid(v)");
    jsonData1.add(person2);

    String query1 =
        "{\n"
            + "    me(func: eq(email, \"ashish@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";
    Mutation mu1 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData1.toString())).build();
    Request request1 =
        Request.newBuilder().addMutations(mu1).setQuery(query1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request1);

    String query2 =
        "{\n"
            + "    me(func: eq(email, \"ashish@dgraph.io\")) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";
    Request request2 = Request.newBuilder().setQuery(query2).build();
    Response response2 = dgraphClient.newTransaction().doRequest(request2);
    String actual2 = response2.getJson().toStringUtf8();
    String expected2 = "{\"me\":[{\"name\":\"wrong\",\"email\":\"ashish@dgraph.io\"}]}";
    assertEquals(actual2, expected2);

    JsonArray jsonData3 = new JsonArray();
    JsonObject person3 = new JsonObject();
    person3.addProperty("uid", "uid(v)");
    person3.addProperty("name", "ashish");
    jsonData3.add(person3);

    Mutation mu3 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData3.toString())).build();
    Request request3 =
        Request.newBuilder().addMutations(mu3).setQuery(query1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request3);

    Response response3 = dgraphClient.newTransaction().query(query2);
    String actual3 = response3.getJson().toStringUtf8();
    String expected3 = "{\"me\":[{\"name\":\"ashish\",\"email\":\"ashish@dgraph.io\"}]}";
    assertEquals(actual3, expected3);

    Mutation mu4 =
        Mutation.newBuilder()
            .setDelNquads(ByteString.copyFromUtf8("uid(v) <name> * .\nuid(v) <email> * ."))
            .setCond("@if(eq(len(v), 1))")
            .build();
    Request request4 =
        Request.newBuilder().addMutations(mu4).setQuery(query1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request4);

    Response response4 = dgraphClient.newTransaction().query(query2);
    String actual4 = response4.getJson().toStringUtf8();
    String expected4 = "{\"me\":[]}";
    assertEquals(actual4, expected4);
  }

  @Test
  public void upsertBlockTestMultipleUID() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .\nname: string @index(exact) .")
            .build();
    dgraphClient.alter(op);

    JsonArray jsonData1 = new JsonArray();
    JsonObject person1 = new JsonObject();
    person1.addProperty("uid", "_:alice");
    person1.addProperty("name", "alice");
    jsonData1.add(person1);

    JsonObject person2 = new JsonObject();
    person2.addProperty("email", "one@dgraph.io");
    person2.addProperty("uid", "_:alice");
    jsonData1.add(person2);

    JsonObject person3 = new JsonObject();
    person3.addProperty("uid", "_:bob");
    person3.addProperty("name", "bob");
    jsonData1.add(person3);

    JsonObject person4 = new JsonObject();
    person4.addProperty("email", "one@dgraph.io");
    person4.addProperty("uid", "_:bob");
    jsonData1.add(person4);

    Mutation mu1 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData1.toString())).build();
    Request request1 = Request.newBuilder().addMutations(mu1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request1);

    JsonArray jsonData2 = new JsonArray();
    JsonObject person5 = new JsonObject();
    person5.addProperty("uid", "uid(v)");
    person5.addProperty("email", "two@dgraph.io");
    jsonData2.add(person5);

    String query2 =
        "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";
    Mutation mu2 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData2.toString())).build();
    Request request2 =
        Request.newBuilder().addMutations(mu2).setQuery(query2).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request2);

    String query3 =
        "{\n"
            + "    me(func: eq(email, \"two@dgraph.io\"), orderasc: name) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";
    Response response3 = dgraphClient.newTransaction().query(query3);
    String actual3 = response3.getJson().toStringUtf8();
    String expected3 =
        "{\"me\":[{\"name\":\"alice\",\"email\":\"two@dgraph.io\"},{\"name\":\"bob\",\"email\":\"two@dgraph.io\"}]}";
    assertEquals(actual3, expected3);
  }

  @Test
  public void upsertBlockTestBulkDelete() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .\nname: string @index(exact) .")
            .build();
    dgraphClient.alter(op);

    JsonArray jsonData1 = new JsonArray();
    JsonObject person1 = new JsonObject();
    person1.addProperty("uid", "_:alice");
    person1.addProperty("name", "alice");
    jsonData1.add(person1);

    JsonObject person2 = new JsonObject();
    person2.addProperty("email", "one@dgraph.io");
    person2.addProperty("uid", "_:alice");
    jsonData1.add(person2);

    JsonObject person3 = new JsonObject();
    person3.addProperty("uid", "_:bob");
    person3.addProperty("name", "bob");
    jsonData1.add(person3);

    JsonObject person4 = new JsonObject();
    person4.addProperty("email", "one@dgraph.io");
    person4.addProperty("uid", "_:bob");
    jsonData1.add(person4);

    Mutation mu1 =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData1.toString())).build();
    Request request1 = Request.newBuilder().addMutations(mu1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request1);

    String query1 =
        "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\"), orderasc: name) {\n"
            + "        name\n"
            + "        email\n"
            + "    }\n"
            + "}\n";

    // Verify if records are successfully inserted.
    Response response1 = dgraphClient.newTransaction().query(query1);
    String actual1 = response1.getJson().toStringUtf8();
    String expected1 =
        "{\"me\":[{\"name\":\"alice\",\"email\":\"one@dgraph.io\"},{\"name\":\"bob\",\"email\":\"one@dgraph.io\"}]}";
    assertEquals(actual1, expected1);

    // DELETE using UPSERT.
    JsonArray jsonData2 = new JsonArray();
    JsonObject person5 = new JsonObject();
    person5.addProperty("uid", "uid(v)");
    person5.addProperty("email", "one@dgraph.io");
    jsonData2.add(person5);

    String query2 =
        "{\n"
            + "    me(func: eq(email, \"one@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";
    Mutation mu2 =
        Mutation.newBuilder().setDeleteJson(ByteString.copyFromUtf8(jsonData2.toString())).build();
    Request request2 =
        Request.newBuilder().addMutations(mu2).setQuery(query2).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request2);

    // GET again using email.
    Response response3 = dgraphClient.newTransaction().query(query1);
    String actual3 = response3.getJson().toStringUtf8();
    String expected3 = "{\"me\":[]}";
    assertEquals(actual3, expected3);
  }

  @Test
  public void upsertBlockTestBulkUpdate() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("name: string @index(exact) .\n" + "branch: string .\n" + "amount: float .")
            .build();
    dgraphClient.alter(op);

    String muStr1 =
        ""
            + "_:user1 <name> \"user1\" .\n"
            + "_:user1 <branch> \"Fuller Street, San Francisco\" .\n"
            + "_:user1 <amount> \"10\" .\n"
            + "_:user2 <name> \"user2\" .\n"
            + "_:user2 <branch> \"Fuller Street, San Francisco\" .\n"
            + "_:user2 <amount> \"100\" .\n"
            + "_:user3 <name> \"user3\" .\n"
            + "_:user3 <branch> \"Fuller Street, San Francisco\" .\n"
            + "_:user3 <amount> \"1000\" .";

    Mutation mu1 = Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(muStr1)).build();
    Request request1 = Request.newBuilder().addMutations(mu1).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request1);

    String query1 =
        "{\n"
            + "  users(func: has(branch)) {\n"
            + "    name\n"
            + "    branch\n"
            + "    amount\n"
            + "  }\n"
            + "}";

    // Verify if records are successfully inserted.
    Response response1 = dgraphClient.newTransaction().query(query1);
    Root root1 = new Gson().fromJson(response1.getJson().toStringUtf8(), Root.class);
    assertEquals(root1.users.size(), 3);
    for (User user : root1.users) {
      assertEquals(user.branch, "Fuller Street, San Francisco");
    }

    // Update using UPSERT
    String muStr2 = "uid(u) <branch> \"Fuller Street, SF\" .";
    String query2 = "{\n" + "    u as var(func: has(branch))\n" + "  }";
    Mutation mu2 = Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(muStr2)).build();
    Request request2 =
        Request.newBuilder().addMutations(mu2).setQuery(query2).setCommitNow(true).build();
    dgraphClient.newTransaction().doRequest(request2);

    // GET again using email.
    Response response3 = dgraphClient.newTransaction().query(query1);
    Root root3 = new Gson().fromJson(response3.getJson().toStringUtf8(), Root.class);
    assertEquals(root3.users.size(), 3);
    for (User user : root3.users) {
      assertEquals(user.branch, "Fuller Street, SF");
    }
  }

  @Test
  public void upsertZeroMutations() {
    Request request1 = Request.newBuilder().setCommitNow(true).build();

    try {
      dgraphClient.newTransaction().doRequest(request1);
    } catch (RuntimeException e) {
      Throwable cause = e;
      Throwable child = cause.getCause();
      while (child != null) {
        cause = child;
        child = cause.getCause();
      }

      assertTrue(cause.getMessage().contains("empty request"));
    }
  }

  @Test
  public void upsertMultipleWithinSingleTransactionTest() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .")
            .build();
    dgraphClient.alter(op);

    JsonArray jsonData = new JsonArray();
    JsonObject person = new JsonObject();
    person.addProperty("uid", "uid(v)");
    person.addProperty("name", "wrong");
    jsonData.add(person);

    JsonObject person2 = new JsonObject();
    person2.addProperty("email", "ashish@dgraph.io");
    person2.addProperty("uid", "uid(v)");
    jsonData.add(person2);

    String query =
        "{\n"
            + "    me(func: eq(email, \"ashish@dgraph.io\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";
    Mutation mu =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData.toString())).build();
    Request request = Request.newBuilder().addMutations(mu).setQuery(query).build();

    Transaction transaction = dgraphClient.newTransaction();
    transaction.doRequest(request);

    try {
      transaction.doRequest(request);
    } catch (RuntimeException e) {
      fail(e.getMessage());
    }

    transaction.discard();
  }

  @Test
  public void upsertTimeoutTest() {
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("email: string @index(exact) @upsert .")
            .build();
    dgraphClient.alter(op);

    JsonArray jsonData = new JsonArray();
    JsonObject person = new JsonObject();
    person.addProperty("uid", "uid(v)");
    person.addProperty("name", "wrong");
    jsonData.add(person);

    JsonObject person2 = new JsonObject();
    person2.addProperty("email", "me@example.com");
    person2.addProperty("uid", "uid(v)");
    jsonData.add(person2);

    String query =
        "{\n"
            + "    me(func: eq(email, \"me@example.com\")) {\n"
            + "        v as uid\n"
            + "    }\n"
            + "}\n";
    Mutation mu =
        Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(jsonData.toString())).build();
    Request request = Request.newBuilder().addMutations(mu).setQuery(query).build();

    Transaction transaction = dgraphClient.newTransaction();
    transaction.doRequest(request, 10, TimeUnit.SECONDS);

    try {
      transaction.doRequest(request);
    } catch (RuntimeException e) {
      fail(e.getMessage());
    }

    transaction.discard();
  }

  static class User {
    String branch;
  }

  static class Root {
    List<User> users;
  }
}
