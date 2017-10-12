/*
 * Copyright 2016 DGraph Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dgraph.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.dgraph.entity.DgraphRequest;

/**
 * Tests for Dgraph client.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public class DgraphClientTest {

    private static DgraphClient dgraphClient;

    private static final String TEST_HOSTNAME = "localhost";
    // This is gRPC port, which runs HTTP2, use "--grpc_port=nnn" to start
    // dgraph
    private static final int TEST_PORT = 9080;

    @BeforeClass
    public static void beforeClass() {
        dgraphClient = GrpcDgraphClient.newInstance(TEST_HOSTNAME, TEST_PORT);
    }

    @AfterClass
    public static void afterClass() {
        dgraphClient.close();
    }

    @Test
    public void testMutationAndQuery() {
        String schema = "mutation { schema { name: string @index(term) . } }";
        String mutation = "mutation { set { _:alice <name> \"Alice\" . \n _:greg <name> \"Greg\" . \n _:alice <follows> _:greg . } }";
        String query = "{ me(func: anyofterms(name, \"Alice Greg\"), orderasc: name) { _uid_ name follows { name } }\n" + "me2(func: anyofterms(name, \"Alice Greg\")) { _uid_ name } }\n";

        dgraphClient.query(schema);
        DgraphResult result = dgraphClient.query(mutation);

        result = dgraphClient.query(query);
        assertNotNull(result);
        JsonObject jsonResult = null;
        try {
            jsonResult = result.toJsonObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(2, jsonResult.getAsJsonArray("me").size());

        final JsonObject resNode = jsonResult.getAsJsonArray("me").get(0).getAsJsonObject();

        assertTrue(resNode.has("_uid_"));
        assertNotNull(resNode.get("_uid_"));

        final JsonArray childrenNodes = resNode.get("follows").getAsJsonArray();
        assertEquals(1, childrenNodes.size());
        final List<String> names = Lists.newArrayListWithCapacity(2);
        for (final JsonElement child : childrenNodes) {
            names.add(child.getAsJsonObject().get("name").getAsString());
        }
        Collections.sort(names);

        assertEquals("Greg", names.get(0));
    }

    @Test
    public void testJsonCreation() {

        DgraphRequest req = new DgraphRequest();
        Map<String, Object> obj = Maps.newHashMap();

        obj.put("name", "myName");
        obj.put("key2", 3.1);
        req.setObject(obj);
        DgraphResult result = dgraphClient.query(req);
        assertNotNull(result);
        Map<String, Long> assignedUids = result.getResponse().getAssignedUidsMap();
        assertEquals(assignedUids.values().size(), 1);
        Long uid = (long) assignedUids.values().toArray()[0];
        assertNotNull(uid);

        String query = "{me(func:uid(" + uid + ")) { uid,key1,key2}}";
        result = dgraphClient.query(query);
        assertNotNull(result);
        JsonObject jsonResult = null;
        try {
            jsonResult = result.toJsonObject();
        } catch (Exception e) {
            e.printStackTrace();

        }
        assertEquals(1, jsonResult.getAsJsonArray("me").size());

    }

    @Test
    public void testJsonUpdate() {

        DgraphRequest createReq = new DgraphRequest();
        Map<String, Object> obj = Maps.newHashMap();

        obj.put("name", "myName");
        obj.put("key2", 3.1);
        createReq.setObject(obj);
        DgraphResult result = dgraphClient.query(createReq);
        assertNotNull(result);
        Map<String, Long> assignedUids = result.getResponse().getAssignedUidsMap();
        assertEquals(assignedUids.values().size(), 1);
        long uid = (long) assignedUids.values().toArray()[0];

        obj.put("_uid_", uid);
        obj.put("name", "newName");

        DgraphRequest updateReq = new DgraphRequest();
        updateReq.setObject(obj);
        result = dgraphClient.query(updateReq);

        String query = "{me(func:uid(" + uid + ")) { uid,name,key2}}";
        result = dgraphClient.query(query);
        assertNotNull(result);
        JsonObject jsonResult = null;
        try {
            jsonResult = result.toJsonObject();
        } catch (Exception e) {
            e.printStackTrace();

        }
        assertEquals(1, jsonResult.getAsJsonArray("me").size());
        JsonObject resNode = jsonResult.getAsJsonArray("me").get(0).getAsJsonObject();

        assertTrue(resNode.has("name"));
        Gson gson = new Gson();
        System.out.println(gson.toJson(resNode));
        JsonElement var = resNode.get("name");
        assertEquals("newName", var.getAsString());

    }

    @Test
    public void testJsonDelete() {

        DgraphRequest createReq = new DgraphRequest();
        Map<String, Object> obj = Maps.newHashMap();

        obj.put("name", "myName");
        obj.put("key2", 3.1);
        createReq.setObject(obj);
        DgraphResult result = dgraphClient.query(createReq);
        assertNotNull(result);
        Map<String, Long> assignedUids = result.getResponse().getAssignedUidsMap();
        assertEquals(assignedUids.values().size(), 1);
        long uid = (long) assignedUids.values().toArray()[0];

        obj.put("_uid_", uid);
        obj.put("name", "newName");

        DgraphRequest updateReq = new DgraphRequest();
        updateReq.deleteObject(obj);
        result = dgraphClient.query(updateReq);

        assignedUids = result.getResponse().getAssignedUidsMap();
        assertEquals(assignedUids.values().size(), 0);

        String query = "{me(func:uid(" + uid + ")) { uid,key1,key2}}";
        result = dgraphClient.query(query);
        assertNotNull(result);
        JsonObject jsonResult = null;
        try {
            jsonResult = result.toJsonObject();
        } catch (Exception e) {
            e.printStackTrace();

        }
        assertTrue(jsonResult.getAsJsonArray("me") == null);

    }
}
