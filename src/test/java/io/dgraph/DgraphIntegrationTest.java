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

import static org.testng.Assert.fail;

import io.dgraph.DgraphProto.Operation;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class DgraphIntegrationTest {
  static final Logger logger = LoggerFactory.getLogger(DgraphIntegrationTest.class);
  static final String TEST_HOSTNAME = "localhost";
  static final int TEST_gRPC_PORT = 9180;
  static final int TEST_HTTP_PORT = 8180;
  protected static DgraphClient dgraphClient;
  private static ManagedChannel channel1, channel2, channel3;

  @BeforeClass
  public static void beforeClass() throws InterruptedException, IOException {
    String baseCertPath = "/home/aman/gocode/src/github.com/dgraph-io/dgraph/tlstest/tls";
    setupTLSClient(baseCertPath);
    // setupClient();

    boolean succeed = false;
    boolean retry;
    do {
      retry = false;

      try {
        // since the cluster is run with ACL turned on by default,
        // we need to login as groot to perform arbitrary operations
        dgraphClient.login("groot", "password");
        succeed = true;
      } catch (RuntimeException e) {
        // check if the error can be retried
        Throwable exception = e;
        while (exception != null) {
          if (exception.getMessage().contains("Please retry")) {
            retry = true;
            break;
          }
          exception = exception.getCause();
        }
      }

      if (retry) {
        System.out.println("Received error, will retry after 1s");
        Thread.sleep(1000);
      }
    } while (retry);

    if (!succeed) {
      fail("Unable to perform the DropAll operation");
    }

    // clean up database
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  private static void setupTLSClient(String baseCertPath) throws IOException {
        // convert PKCS#1 to PKCS#8
        PEMParser pemParser = new PEMParser(new FileReader(baseCertPath + "/client.acl.key"));
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        Object object = pemParser.readObject();
        KeyPair pair = converter.getKeyPair((PEMKeyPair) object);
        PrivateKey priv = pair.getPrivate();
        byte[] privBytes = priv.getEncoded();

        // PEM object from PKCS#8
        PemObject pemObject = new PemObject("RSA PRIVATE KEY", privBytes);
        StringWriter stringWriter = new StringWriter();
        PemWriter pemWriter = new PemWriter(stringWriter);
        pemWriter.writeObject(pemObject);
        pemWriter.close();
        String pemString = stringWriter.toString();

    // Setup SSL context with keys and certificates
    SslContextBuilder builder = GrpcSslContexts.forClient();
    builder.trustManager(new File(baseCertPath + "/ca.crt"));
        builder.keyManager(
            new FileInputStream(baseCertPath + "/client.acl.crt"),
            new ByteArrayInputStream(pemString.getBytes(StandardCharsets.UTF_8)));
    SslContext sslContext = builder.build();

    channel1 = NettyChannelBuilder.forAddress("localhost", 9180).sslContext(sslContext).build();
    DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);

    channel2 = NettyChannelBuilder.forAddress("localhost", 9182).sslContext(sslContext).build();
    DgraphGrpc.DgraphStub stub2 = DgraphGrpc.newStub(channel2);

    channel3 = NettyChannelBuilder.forAddress("localhost", 9183).sslContext(sslContext).build();
    DgraphGrpc.DgraphStub stub3 = DgraphGrpc.newStub(channel3);

    dgraphClient = new DgraphClient(stub1, stub2, stub3);
  }

  private static void setupClient() {
    channel1 = ManagedChannelBuilder.forAddress("localhost", 9180).usePlaintext().build();
    DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);

    channel2 = ManagedChannelBuilder.forAddress("localhost", 9182).usePlaintext().build();
    DgraphGrpc.DgraphStub stub2 = DgraphGrpc.newStub(channel2);

    channel3 = ManagedChannelBuilder.forAddress("localhost", 9183).usePlaintext().build();
    DgraphGrpc.DgraphStub stub3 = DgraphGrpc.newStub(channel3);

    dgraphClient = new DgraphClient(stub1, stub2, stub3);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    // Shutdown channel connections
    channel1.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    channel2.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    channel3.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    // Or, alternatively, shutdown channels from the client
    dgraphClient.shutdown();
  }
}
