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

import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Implementation of a DgraphClientStub using grpc.
 *
 * Returns DgraphClientStub, that can be used to query/mutate Slash GraphQL endpoints
 *
 * Takes the Slash GraphQL Endpoint and and apiKey as input.
 * 
 * @author Abhimanyu Singh Gaur
 * @author Neeraj Battan
 */
public class DgraphClientStub {
  public static DgraphGrpc.DgraphStub fromSlashEndpoint(String slashEndpoint, String apiKey)
      throws MalformedURLException {
    URL url = new URL(slashEndpoint);
    String[] parts = url.getHost().split("[.]", 2);
    if (parts.length < 2) {
      throw new MalformedURLException("Invalid Slash URL.");
    }
    String grpcAddress = parts[0] + ".grpc." + parts[1];

    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), apiKey);
    return MetadataUtils.attachHeaders(
        DgraphGrpc.newStub(
            ManagedChannelBuilder.forAddress(grpcAddress, 443).useTransportSecurity().build()),
        metadata);
  }
}
