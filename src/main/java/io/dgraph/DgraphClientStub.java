package io.dgraph;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.net.MalformedURLException;
import java.net.URL;

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
