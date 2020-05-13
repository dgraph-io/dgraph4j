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

import io.dgraph.DgraphProto.Operation;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class DgraphIntegrationTest {
  static final Logger logger = LoggerFactory.getLogger(DgraphIntegrationTest.class);
  static final String TEST_HOSTNAME = "localhost";
  static final int TEST_PORT = 9180;
  protected static DgraphClient dgraphClient;
  private static ManagedChannel channel1;

  @BeforeClass
  public static void beforeClass() throws InterruptedException, SSLException {
    SslContextBuilder builder = GrpcSslContexts.forClient();
    builder.trustManager(new File("/home/dmai/dgraph/tls/ca.crt"));
    SslContext sslContext = builder.build();

    channel1 = NettyChannelBuilder.forAddress("localhost", 9180).sslContext(sslContext).build();
    DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);

    dgraphClient = new DgraphClient(stub1);

    // clean up database
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    channel1.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
