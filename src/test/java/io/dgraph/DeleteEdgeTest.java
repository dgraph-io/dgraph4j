/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class DeleteEdgeTest extends DgraphIntegrationTest {
  @Test
  public void deleteEdgesTest() {
    School school = new School();
    school.name = "Crown Public School";
    List<School> schools = new ArrayList<>();
    schools.add(school);

    Person bob = new Person();
    bob.name = "Bob";
    bob.age = 24;

    Person charlie = new Person();
    charlie.name = "Charlie";
    charlie.age = 29;

    List<Person> friends = new ArrayList<>();
    friends.add(bob);
    friends.add(charlie);

    // Create Person
    Person alice = new Person();
    alice.uid = "_:alice";
    alice.name = "Alice";
    alice.age = 26;
    alice.married = true;
    alice.location = "Riley Street";
    alice.schools = schools;
    alice.friends = friends;

    Operation op = Operation.newBuilder().setSchema("age: int .\nmarried: bool .").build();
    dgraphClient.alter(op);

    Gson gson = new Gson();
    Mutation mu =
        Mutation.newBuilder()
            .setSetJson(ByteString.copyFromUtf8(gson.toJson(alice)))
            .setCommitNow(true)
            .build();
    Response resp = dgraphClient.newTransaction().mutate(mu);
    String uid = resp.getUidsOrThrow("alice");

    String q =
        "{\n"
            + "  me(func: uid(%s)) {\n"
            + "   uid\n"
            + "   name\n"
            + "   age\n"
            + "   loc\n"
            + "   married\n"
            + "   friends {\n"
            + "    uid\n"
            + "    name\n"
            + "    age\n"
            + "   }\n"
            + "   schools {\n"
            + "    uid\n"
            + "    name\n"
            + "   }\n"
            + "  }\n"
            + " }";
    q = String.format(q, uid);
    resp = dgraphClient.newTransaction().query(q);
    Root r = gson.fromJson(resp.getJson().toStringUtf8(), Root.class);
    assertEquals(r.me.get(0).friends.size(), 2);

    mu =
        Helpers.deleteEdges(
            Mutation.newBuilder().setCommitNow(true).build(), uid, "friends", "loc");
    dgraphClient.newTransaction().mutate(mu);

    resp = dgraphClient.newTransaction().query(q);
    r = gson.fromJson(resp.getJson().toStringUtf8(), Root.class);
    assertNull(r.me.get(0).friends);
  }

  static class School {
    String name;
  }

  static class Person {
    String uid;
    String name;
    int age;
    boolean married;
    List<Person> friends;
    String location;
    List<School> schools;
  }

  static class Root {
    List<Person> me;
  }
}
