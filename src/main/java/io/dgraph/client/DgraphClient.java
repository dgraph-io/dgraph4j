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

import io.dgraph.entity.DgraphRequest;
import io.dgraph.entity.Node;
import io.dgraph.proto.AssignedIds;

/**
 * Interface of a Dgraph client.
 *
 * @author Edgar Rodriguez-Diaz
 * @version 0.0.1
 */
public interface DgraphClient {

	/**
	 * Executes a query in Dgraph.
	 *
	 * @param theQueryString
	 *            the query string
	 * @return the results as string
	 */
	DgraphResult query(String theQueryString);

	DgraphResult query(DgraphRequest request);
	
	/**
	 * Closes resources used by the client.
	 */
	void close();

    Node NodeBlank(String varName);

    AssignedIds assignUid(long count);

    Node NodeUidVar(String name);

}
