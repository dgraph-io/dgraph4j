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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Tests for Dgraph client.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public class DgraphClientTest {

	private static DgraphClient dgraphClient;

	private static final String TEST_HOSTNAME = "192.168.56.101";
	private static final int TEST_PORT = 8080;

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
		String schema = "mutation { schema { name: string @index . } }";
		String mutation = "mutation { set {"
				+ "    <alice> <name> \"Alice\" . <greg> <name> \"Greg\" . <alice> <follows> <greg> . " + "  }" + "}";
		String query = "{ me(func: anyofterms(name, \"Alice Greg\"), orderasc: name) "
				+ "{ _uid_ name follows { name }}" + " me2(func: anyofterms(name, \"Alice Greg\")) { _uid_ name } }";

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
		assertEquals(2, childrenNodes.size());
		final List<String> names = Lists.newArrayListWithCapacity(2);
		for (final JsonElement child : childrenNodes) {
			names.add(child.getAsJsonObject().get("name").getAsString());
		}
		Collections.sort(names);

		assertEquals("Bob", names.get(0));
		assertEquals("Greg", names.get(1));
	}

	// @Test
	// public void testMutationAndQueryTwoLevel() {
	// final DgraphResult result = dgraphClient
	// .query("mutation { \n" + " set { \n" + " <alice> <follows> <bob> . \n"
	// + " <alice> <name> \"Alice\" . \n" + " <bob> <name> \"Bob\" . \n" + " }
	// \n"
	// + "} \n" + "query { \n" + " debug(_xid_: alice) { \n" + " name _xid_
	// follows { \n"
	// + " name _xid_ follows { \n" + " name _xid_ \n" + " } \n"
	// + " } \n" + " } \n" + "}");
	// assertNotNull(result);
	// final JsonObject jsonResult = result.toJsonObject();
	// logger.info(jsonResult.toString());
	// assertEquals(1, jsonResult.getAsJsonArray("debug").size());
	//
	// final JsonObject gregNode =
	// jsonResult.getAsJsonArray("debug").get(0).getAsJsonObject();
	// assertEquals("alice",
	// gregNode.getAsJsonPrimitive("_xid_").getAsString());
	// assertEquals("Alice", gregNode.getAsJsonPrimitive("name").getAsString());
	// }
}
