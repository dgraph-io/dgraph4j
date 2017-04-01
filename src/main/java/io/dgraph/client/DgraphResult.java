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

import com.google.gson.JsonObject;

import io.dgraph.proto.Response;

/**
 * This abstract class wraps the actual response from Dgraph. An abstract method
 * for converting the response to a JsonObject is defined.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public abstract class DgraphResult {

	private Response root;

	protected DgraphResult(final Response theRootResult) {
		root = theRootResult;
	}

	public Response getResponse() {
		return root;
	}

	public abstract JsonObject toJsonObject();
}
