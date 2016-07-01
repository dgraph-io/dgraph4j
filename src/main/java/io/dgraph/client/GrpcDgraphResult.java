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

import java.util.Stack;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Implementation of DgraphResult using Grpcresponse.Response.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public class GrpcDgraphResult extends DgraphResult<Graphresponse.Response> {

    private GrpcDgraphResult(final Graphresponse.Response theRootResult) {
        super(theRootResult);
    }

    @Override
    public JsonObject toJsonObject() {
        final Stack<Graphresponse.Node> nodes = new Stack<>();

        final Graphresponse.Node rootNode = getResponse().getN();
        JsonObject rootJson = null;

        nodes.push(rootNode);
        while (!nodes.isEmpty()) {
            final Graphresponse.Node aNode = nodes.pop();
            final JsonObject jsonNode = nodeToJson(aNode);
            rootJson = (rootJson == null) ? jsonNode : rootJson;  // keep track of the root

            for (Graphresponse.Node child : aNode.getChildrenList()) {
                final JsonObject childJson = nodeToJson(child);
                nodes.push(child);

                if (!jsonNode.has(child.getAttribute())) {
                    jsonNode.add(child.getAttribute(), new JsonArray());
                }
                jsonNode.getAsJsonArray(child.getAttribute())
                        .add(childJson);
            }
        }

        final JsonObject jsonQueryResults = new JsonObject();
        final JsonArray jsonResultElements = new JsonArray();
        jsonResultElements.add(rootJson);
        jsonQueryResults.add(rootNode.getAttribute(), jsonResultElements);

        final JsonObject jsonLatency = new JsonObject();
        jsonLatency.addProperty("pb", getResponse().getL().getPb());
        jsonLatency.addProperty("parsing", getResponse().getL().getParsing());
        jsonLatency.addProperty("processing", getResponse().getL().getProcessing());
        // TODO: add total
        jsonQueryResults.add("server_latency", jsonLatency);

        return jsonQueryResults;
    }

    private JsonObject nodeToJson(Graphresponse.Node theNode) {
        final JsonObject jsonNode = new JsonObject();

        jsonNode.addProperty("_uid_", "0x" + Long.toHexString(theNode.getUid()));
        if (!Strings.isNullOrEmpty(theNode.getXid())) {
            jsonNode.addProperty("_xid_", theNode.getXid());
        }
        for (Graphresponse.Property aProp : theNode.getPropertiesList()) {
            // TODO: this requires a way to figure out dynamically the decoder
            // TODO: using String for now
            if (!aProp.getVal().isEmpty()) {
                jsonNode.addProperty(aProp.getProp(),
                                     ValueDecoders.STRING_UTF8.decode(aProp.getVal()
                                                                           .toByteArray()));
            }
        }
        return jsonNode;
    }

    public static GrpcDgraphResult newInstance(final Graphresponse.Response theResponse) {
        return new GrpcDgraphResult(theResponse);
    }
}
