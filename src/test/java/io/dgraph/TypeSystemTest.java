package io.dgraph;

import static org.testng.AssertJUnit.assertEquals;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Response;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class TypeSystemTest extends DgraphIntegrationTest {
  private String schema =
      ""
          + "name: string @index(term, exact) .\n"
          + "age: int .\n"
          + ""
          + "type Person {\n"
          + "    name\n"
          + "    age\n"
          + "}\n";

  @Test
  public void testTypeFunction() {
    Operation op = Operation.newBuilder().setSchema(schema).build();
    dgraphClient.alter(op);
    AlterUtils.waitForIndexing(dgraphClient, "name", Arrays.asList("term", "exact"), false, false);

    String muStr =
        "_:animesh <name> \"Animesh\" .\n"
            + "_:animesh <age> \"24\" .\n"
            + "_:animesh <dgraph.type> \"Person\" .";
    Mutation mu =
        Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8(muStr))
            .setCommitNow(true)
            .build();
    dgraphClient.newTransaction().mutate(mu);

    String query =
        "{\n" + "    me(func: type(Person)) {\n" + "        expand(_all_)\n" + "    }\n" + "}";
    Response response = dgraphClient.newTransaction().query(query);
    Root root = new Gson().fromJson(response.getJson().toStringUtf8(), Root.class);
    assertEquals(root.me.size(), 1);
  }

  @Test
  public void testTypeDeletion() {
    Operation op = Operation.newBuilder().setSchema(schema).build();
    dgraphClient.alter(op);
    AlterUtils.waitForIndexing(dgraphClient, "name", Arrays.asList("term", "exact"), false, false);

    String muStr = "" + "_:animesh <name> \"Animesh\" .\n" + "_:animesh <age> \"24\" .";
    Mutation mu =
        Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8(muStr))
            .setCommitNow(true)
            .build();
    dgraphClient.newTransaction().mutate(mu);

    // Delete using S * *
    String upsertQuery = "" + "{\n" + "    u as var(func: eq(name, \"Animesh\"))\n" + "}";
    Mutation upsertMutation =
        Mutation.newBuilder().setDelNquads(ByteString.copyFromUtf8("uid(u) * * .")).build();
    Request deleteRequest =
        Request.newBuilder()
            .addMutations(upsertMutation)
            .setQuery(upsertQuery)
            .setCommitNow(true)
            .build();
    dgraphClient.newTransaction().doRequest(deleteRequest);

    // This shouldn't work because no type is associated to the node.
    String verifyQuery =
        "" + "{\n" + "    me(func: eq(name, \"Animesh\")) {\n" + "        name\n" + "    }\n" + "}";
    Response beforeResponse = dgraphClient.newTransaction().query(verifyQuery);
    Root beforeRoot = new Gson().fromJson(beforeResponse.getJson().toStringUtf8(), Root.class);
    assertEquals(beforeRoot.me.size(), 1);

    // Associate type with the node so that the mutation works now.
    Mutation typeMutation =
        Mutation.newBuilder()
            .setSetNquads(ByteString.copyFromUtf8("uid(u) <dgraph.type> \"Person\" ."))
            .build();
    Request typeRequest =
        Request.newBuilder()
            .addMutations(typeMutation)
            .setQuery(upsertQuery)
            .setCommitNow(true)
            .build();
    dgraphClient.newTransaction().doRequest(typeRequest);

    // Deletion using S * * should work now
    dgraphClient.newTransaction().doRequest(deleteRequest);
    Response afterResponse = dgraphClient.newTransaction().query(verifyQuery);
    Root afterRoot = new Gson().fromJson(afterResponse.getJson().toStringUtf8(), Root.class);
    assertEquals(afterRoot.me.size(), 0);
  }

  private static class Person {}

  static class Root {
    List<Person> me;
  }
}
