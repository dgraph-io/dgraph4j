/*
 * Copyright (C) 2020 Dgraph Labs, Inc. and Contributors
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
 * DgraphClientStub provides an easy API to get pre-built gRPC stubs which can be used to construct
 * a DgraphClient/DgraphAsyncClient.
 *
 * @author Abhimanyu Singh Gaur
 * @author Neeraj Battan
 */
public class DgraphClientStub {
  private static final String gRPC_AUTHORIZATION_HEADER_NAME = "authorization";

  /**
   * Creates a gRPC stub that can be used to construct clients to connect with Slash GraphQL.
   *
   * @param slashEndpoint The url of the Slash GraphQL endpoint. Example:
   *     https://your-slash-instance.cloud.dgraph.io/graphql
   * @param apiKey The API key used to connect to your Slash GraphQL instance.
   * @return A new DgraphGrpc.DgraphStub object to be used with DgraphClient/DgraphAsyncClient.
   */
  public static DgraphGrpc.DgraphStub fromSlashEndpoint(String slashEndpoint, String apiKey)
      throws MalformedURLException {
    String[] parts = new URL(slashEndpoint).getHost().split("[.]", 2);
    if (parts.length < 2) {
      throw new MalformedURLException("Invalid Slash URL.");
    }
    String gRPCAddress = parts[0] + ".grpc." + parts[1];

    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of(gRPC_AUTHORIZATION_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER), apiKey);
    return MetadataUtils.attachHeaders(
        DgraphGrpc.newStub(
            ManagedChannelBuilder.forAddress(gRPCAddress, 443).useTransportSecurity().build()),
        metadata);
  }
}
