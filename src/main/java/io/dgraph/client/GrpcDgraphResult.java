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

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.dgraph.proto.Node;
import io.dgraph.proto.Property;
import io.dgraph.proto.Response;
import io.dgraph.proto.Value;

/**
 * Implementation of DgraphResult using Grpcresponse.Response.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public class GrpcDgraphResult extends DgraphResult {

	private GrpcDgraphResult(final Response theRootResult) {
		super(theRootResult);
	}

	@Override
	public JsonObject toJsonObject() {
		JsonObject results = new JsonObject();

		for (int i = 0; i < getResponse().getNCount(); i++) {
			Node node = getResponse().getN(i);
			childrenToJson(results, node);
		}

		JsonObject jsonLatency = new JsonObject();
		jsonLatency.addProperty("pb", getResponse().getL().getPb());
		jsonLatency.addProperty("parsing", getResponse().getL().getParsing());
		jsonLatency.addProperty("processing", getResponse().getL().getProcessing());
		// TODO: add total

		results.add("server_latency", jsonLatency);

		return results;
	}

	private void childrenToJson(JsonObject json, Node node) {
		String attr = null;
		JsonArray list = new JsonArray();
		for (Node child : node.getChildrenList()) {
			if (attr == null) {
				attr = child.getAttribute();
			}
			list.add(childToJson(child));
		}
		if (attr != null) {
			json.add(attr, list);
		}
	}

	private JsonObject childToJson(Node node) {
		JsonObject jsonNode = new JsonObject();

		if (node.getPropertiesCount() > 0) {
			for (Property prop : node.getPropertiesList()) {
				if (prop.getValue().isInitialized()) {
					jsonNode.add(prop.getProp(), valueToJsonElem(prop.getValue()));
				}
			}
		}
		if (node.getChildrenCount() > 0) {
			childrenToJson(jsonNode, node);
		}
		return jsonNode;
	}

	private JsonElement valueToJsonElem(Value value) {
		switch (value.getValCase().getNumber()) {
		case Value.BOOL_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getBoolVal());
		case Value.BYTES_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getBytesVal().toString());
		case Value.DATE_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getDateVal().toString());
		case Value.DATETIME_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getDatetimeVal().toString());
		case Value.DEFAULT_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getDefaultVal().toString());
		case Value.DOUBLE_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getDoubleVal());
		case Value.GEO_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getGeoVal().toString());
		case Value.INT_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getIntVal());
		case Value.PASSWORD_VAL_FIELD_NUMBER:
			break;
		case Value.STR_VAL_FIELD_NUMBER:
			return new JsonPrimitive(value.getStrVal());
		}
		return null;
	}

	public static GrpcDgraphResult newInstance(final Response theResponse) {
		return new GrpcDgraphResult(theResponse);
	}
}
